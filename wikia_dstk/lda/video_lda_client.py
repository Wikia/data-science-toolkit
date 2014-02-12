import requests
import json
import argparse
import os
import time
from . import normalize, unis_bis_tris, video_json_key, log
from multiprocessing import Pool
from boto import connect_s3
from boto.ec2 import connect_to_region


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--build', dest='build', action='store_true',
                        help="Build new feature set for S3")
    parser.add_argument('--build-only', dest='build_only', action='store_true', default=False,
                        help="Build new feature set for S3")
    parser.add_argument('--ami', dest='ami', type=str,
                        help='The AMI to launch')
    parser.add_argument('--num-nodes', dest='node_count', type=int,
                        default=20,
                        help="Number of worker nodes to launch")
    parser.add_argument('--num-processes', dest='num_processes', type=int,
                        default=8,
                        help="Number of processes to run data extraction on")
    parser.add_argument('--num-topics', dest='num_topics', type=int, action='store',
                        default=os.getenv('NUM_TOPICS', 999),
                        help="The number of topics for the model to use")
    parser.add_argument('--path-prefix', dest='path_prefix', type=str, action='store',
                        default=os.getenv('PATH_PREFIX', '/mnt/'),
                        help="Where to save the model")
    parser.add_argument('--max-topic-frequency', dest='max_topic_frequency', type=int,
                        default=os.getenv('MAX_TOPIC_FREQUENCY', 50000),
                        help="Threshold for number of videos a given topic appears in")
    parser.add_argument('--model-prefix', dest='model_prefix', type=str,
                        default=os.getenv('MODEL_PREFIX', time.time()),
                        help="Prefix to uniqueify model")
    parser.add_argument('--s3-prefix', dest='s3_prefix', type=str,
                        default=os.getenv('S3_PREFIX', "models/video/"),
                        help="Prefix on s3 for model location")
    parser.add_argument('--auto-launch', dest='auto_launch', type=bool,
                        default=os.getenv('AUTOLAUNCH_NODES', True),
                        help="Whether to automatically launch distributed nodes")
    parser.add_argument('--instance-count', dest='instance_count', type=int,
                        default=os.getenv('NODE_INSTANCES', 20),
                        help="Number of node instances to launch")
    parser.add_argument('--node-ami', dest='node_ami', type=str,
                        default=os.getenv('NODE_AMI', "ami-40701570"),
                        help="AMI of the node machines")
    parser.add_argument('--dont-terminate-on-complete', dest='terminate_on_complete', action='store_false',
                        default=os.getenv('TERMINATE_ON_COMPLETE', True),
                        help="Prevent terminating this instance")
    return parser.parse_args()


def doc_to_vectors(doc):
    try:
        data = [d.encode('utf-8') for d in
                [doc[u'id']]
                + unis_bis_tris(doc[u'title_en'].replace(u'File:', u''))
                + map(normalize, doc.get(u'video_actors_txt', []))
                + map(normalize, doc.get(u'video_tags_txt', []))
                + map(normalize, doc.get(u'categories_mv_en', []))
                + map(normalize, doc.get(u'video_tags_txt', []))
                + map(normalize, doc.get(u'video_genres_txt', []))
                + [ubt for li in doc.get(u'video_description_txt', []) for ubt in unis_bis_tris(li)]
                + [ubt for li in doc.get(u'html_media_extras_txt', []) for ubt in unis_bis_tris(li)]
                ]
        return dict([(data[0], data[1])])
    except (IndexError, TypeError) as e:
        log(e)
        return []


def etl_concurrent(pool):
    """
    Asynchronously handle ETL to reduce wait time due to HTTP request blocking
    """
    log("Extracting data...")
    params = {'wt': 'json', 'rows': 0, 'fl': '*', 'q': 'wid:298117 AND is_video:true'}
    response = requests.get('http://search-s10:8983/solr/main/select', params=params).json()
    log(response['response']['numFound'], "videos")
    r = pool.map_async(get_docs, range(0, response['response']['numFound'], 500))
    r.wait()
    docs = [doc for docset in r.get() for doc in docset]
    log("Got all docs, now building features")
    doclen = len(docs)
    features = {}
    for i in range(0, doclen, 5000):
        log("%.2f%%" % (float(i)/float(doclen) * 100))
        map(features.update, pool.map_async(doc_to_vectors, docs[i:i+5000]))
    return features


def get_docs(start):
    """
    Functional core of ETL
    """
    log(start)
    return requests.get('http://search-s10:8983/solr/main/select',
                        params={'wt': 'json',
                        'start': start,
                        'rows': 500,
                        'fl': '*',
                        'q': 'wid:298117 AND is_video:true'}).json().get('response', {}).get('docs', [])


def etl(pool, start=0, dataset=[]):
    """
    Recursive is nice but not actually fast
    """
    params = {'wt': 'json', 'start': start, 'rows': 500, 'fl': '*', 'q': 'wid:298117 AND is_video:true'}
    response = requests.get('http://search-s10:8983/solr/main/select', params=params).json()
    dataset += pool.map(doc_to_vectors, response['response']['docs'])
    if start <= response['response']['numFound']:
        return etl(pool, start=start+500, dataset=dataset)
    return dataset


def data_to_s3(num_processes):
    """
    Store video features on S3 -- a prerequisite for running
    """
    log("Uploading to S3")
    b = connect_s3().get_bucket('nlp-data')
    k = b.new_key(video_json_key)
    k.set_contents_from_string(json.dumps(etl_concurrent(Pool(processes=num_processes)), ensure_ascii=False))


def user_data_from_args(args):
    return ("""#!/usr/bin/bash
mkdir -p /mnt/
export NUM_TOPICS=%d
export MAX_TOPIC_FREQUENCY=%d
export MODEL_PREFIX="%s"
export S3_PREFIX="%s"
export NODE_INSTANCES=%d
export NODE_AMI="%s"
python -m wikia_dstk.lda.video_lda_server.py
    """ % (args.num_topics, args.max_topic_frequency, args.model_prefix,
           args.s3_prefix, args.node_count, args.ami))


def main():
    args = get_args()
    if args.build:
        start = time.time()
        data_to_s3(args.num_processes)
        log("Finished upload to S3 in %d seconds" % (time.time() - start))
    if not args.build_only:
        log("Running LDA, which will auto-terminate upon completion")
        connection = connect_to_region('us-west-2')
        connection.run_instances(args.ami,
                                 instance_type='m2.4xlarge',
                                 user_data=user_data_from_args(args),
                                 subnet_id='subnet-e4d087a2',
                                 security_group_ids=['sg-72190a10'])

if __name__ == '__main__':
    main()