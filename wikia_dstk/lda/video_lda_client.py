import requests
import json
import argparse
import os
import time
from . import normalize, unis_bis_tris, video_json_key
from boto import connect_s3
from boto.ec2 import connect_to_region


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--build', dest='build', type=bool, action='store_true',
                        help="Build new feature set for S3")
    parser.add_argument('--ami', dest='ami', type=str,
                        help='The AMI to launch')
    parser.add_argument('--num-nodes', dest='node_count', type=int,
                        default=20,
                        help="Number of worker nodes to launch")
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
    parser.add__argument('--dont-terminate-on-complete', dest='terminate_on_complete', action='store_false',
                         default=os.getenv('TERMINATE_ON_COMPLETE', True),
                         help="Prevent terminating this instance")
    return parser.parse_args()


def etl(start=0, dataset=[]):
    params = {'wt': 'json', 'start': start, 'rows': 500, 'fl': '*', 'q': 'wid:298117 AND is_video:true'}
    response = requests.get('http://search-s10:8983/solr/main/select', params=params).json()
    for doc in response['response']['docs']:
        data = ([doc[u'id']]
                + unis_bis_tris(doc[u'title_en'].replace(u'File:', u''))
                + map(normalize, doc.get(u'video_actors_txt', []))
                + map(normalize, doc.get(u'video_tags_txt', []))
                + map(normalize, doc.get(u'categories_mv_en', []))
                + map(normalize, doc.get(u'video_tags_txt', []))
                + map(normalize, doc.get(u'video_genres_txt', []))
                + unis_bis_tris(doc.get(u'video_description_txt', ''))
                + unis_bis_tris(doc.get(u'html_media_extras_txt', ''))
                )
        dataset += [d.encode('utf-8') for d in data]
    if start <= response['response']['numFound']:
        return etl(start + 500, dataset=dataset)
    return dataset


def data_to_s3():
    b = connect_s3().get_bucket('nlp-data')
    k = b.new_key(video_json_key)
    k.set_contents_from_string(json.dumps(etl(), ensure_ascii=False))


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
        data_to_s3()
    connection = connect_to_region('us-west-2')
    connection.run_instances(args.ami, instance_type='m2.4xlarge')  # user-data script to run video_lda_server

if __name__ == '__main__':
    main()