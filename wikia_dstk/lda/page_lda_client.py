import argparse
import json
import os
import requests
import time
from boto import connect_s3
from datetime import datetime
from multiprocessing import Pool
from . import normalize, run_server_from_args
from .. import log

WIKI_ID = None


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--build', dest='build', action='store_true',
                        default=False,
                        help="Build new feature set for S3")
    parser.add_argument('--build-only', dest='build_only', action='store_true',
                        default=False,
                        help="Build new feature set for S3")
    parser.add_argument('--overwrite-existing-model', dest='overwrite',
                        action='store_true', default=False,
                        help="Overwrite existing LDA model on S3")
    parser.add_argument('--wiki-id', dest='wiki_id', type=str,
                        help="The wiki ID for which to generate a topic model")
    parser.add_argument('--ami', dest='ami', type=str,
                        default="ami-f4d0bfc4",
                        help='The AMI to launch')
    parser.add_argument('--num-nodes', dest='node_count', type=int,
                        default=20,
                        help="Number of worker nodes to launch")
    parser.add_argument('--num-processes', dest='num_processes', type=int,
                        default=8,
                        help="Number of processes to run data extraction on")
    parser.add_argument('--num-topics', dest='num_topics', type=int,
                        action='store', default=os.getenv('NUM_TOPICS', 999),
                        help="The number of topics for the model to use")
    parser.add_argument('--path-prefix', dest='path_prefix', type=str,
                        action='store',
                        default=os.getenv('PATH_PREFIX', '/mnt/'),
                        help="Where to save the model")
    parser.add_argument('--max-topic-frequency', dest='max_topic_frequency',
                        type=int, default=os.getenv('MAX_TOPIC_FREQUENCY', 500),
                        help="Threshold for number of pages a given topic " +
                        "appears in")
    parser.add_argument('--model-prefix', dest='model_prefix', type=str,
                        default=os.getenv(
                            'MODEL_PREFIX', datetime.strftime(
                                datetime.now(), '%Y-%m-%d-%H-%M')),
                        help="Prefix to uniqueify model")
    parser.add_argument('--s3-prefix', dest='s3_prefix', type=str,
                        default=os.getenv('S3_PREFIX', "models/page/"),
                        help="Prefix on s3 for model location")
    parser.add_argument('--auto-launch', dest='auto_launch', type=bool,
                        default=os.getenv('AUTOLAUNCH_NODES', True),
                        help="Whether to automatically launch distributed nodes")
    parser.add_argument('--instance-count', dest='instance_count', type=int,
                        default=os.getenv('NODE_INSTANCES', 20),
                        help="Number of node instances to launch")
    parser.add_argument('--node-ami', dest='node_ami', type=str,
                        default=os.getenv('NODE_AMI', "ami-d6e785e6"),
                        help="AMI of the node machines")
    parser.add_argument('--dont-terminate-on-complete',
                        dest='terminate_on_complete', action='store_false',
                        default=os.getenv('TERMINATE_ON_COMPLETE', True),
                        help="Prevent terminating this instance")
    parser.add_argument('--master-ip', dest='master_ip',
                        default='54.200.131.148',
                        help="The elastic IP address to associate with the " +
                        "master server")
    parser.add_argument('--killable', dest='killable', action='store_true',
                        default=False,
                        help="Keyboard interrupt terminates master")
    parser.add_argument('--git-ref', dest='git_ref', default='master',
                        help="The git ref to use when deploying")
    return parser.parse_args()


def doc_to_vectors(doc):
    try:
        data = [d.encode('utf-8') for d in [doc[u'id']] +
                map(normalize, doc.get(u'headings_mv_en', [])) +
                map(normalize, doc.get(u'categories_mv_en', []))]
        return dict([(data[0], data[1:])])
    except (IndexError, TypeError) as e:
        log(e)
        return []


def etl_concurrent(pool):
    """
    Asynchronously handle ETL to reduce wait time due to HTTP request blocking
    """
    log('Extracting data...')
    params = {
        'wt': 'json', 'rows': 0, 'fl': 'id,headings_mv_en,categories_mv_en',
        'q': 'wid:%s' % WIKI_ID}
    response = requests.get(
        'http://search-s10:8983/solr/main/select', params=params).json()
    log(response['response']['numFound'], 'docs')
    r = pool.map_async(get_docs,
                       range(0, response['response']['numFound'], 500))
    r.wait()
    docs = [doc for docset in r.get() for doc in docset]
    log('Got all docs, now building features')
    doclen = len(docs)
    features = {}
    for i in range(0, doclen, 5000):
        log('%.2f%%' % (float(i)/float(doclen) * 100))
        map(features.update,
            pool.map_async(doc_to_vectors, docs[i:i+5000]).get())
    return features


def get_docs(start):
    """
    Functional core of ETL
    """
    log(start)
    return requests.get(
        'http://search-s10:8983/solr/main/select',
        params={'wt': 'json', 'start': start, 'rows': 500,
                'fl': 'id,headings_mv_en,categories_mv_en',
                'q': 'wid:%s' % WIKI_ID}
        ).json().get('response', {}).get('docs', [])


def data_to_s3(num_processes):
    """
    Store page features on S3 -- a prerequisite for running
    """
    log('Uploading to S3')
    b = connect_s3().get_bucket('nlp-data')
    k = b.new_key('feature-data/page-%s.json' % WIKI_ID)
    k.set_contents_from_string(json.dumps(
        etl_concurrent(Pool(processes=num_processes)), ensure_ascii=False))


def main():
    global WIKI_ID
    args = get_args()
    WIKI_ID = args.wiki_id
    if args.build:
        start = time.time()
        data_to_s3(args.num_processes)
        log('Finished upload to S3 in %d seconds' % (time.time() - start))
    if not args.build_only:
        run_server_from_args(
            args, 'wikia_dstk.lda.page_lda_server',
            user_data_extras='export WIKI_ID="%s"' % WIKI_ID)


if __name__ == '__main__':
    main()
