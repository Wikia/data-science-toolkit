import requests
import json
from . import normalize, unis_bis_tris, video_json_key
import argparse
from boto import connect_s3
from boto.ec2 import connect_to_region


parser = argparse.ArgumentParser()
parser.add_argument('--build', dest='build', type=bool, action='store_true',
                    help="Build new feature set for S3")
parser.add_argument('--ami', dest='ami', type=str,
                    help='The AMI to launch')


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

if __name__ == '__main__':
    args = parser.parse_args()
    if args.build:
        data_to_s3()
    connection = connect_to_region('us-west-2')
    connection.run_instances(args.ami, instance_type='m2.4xlarge')  # user-data script to run video_lda_server
