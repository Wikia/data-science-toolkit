from __future__ import division
import requests
from boto.ec2 import connect_to_region
from wikia_dstk import get_argparser_from_config, argstring_from_namespace
from ...loadbalancing import run_instances_lb
from config import config


def get_args():
    ap = get_argparser_from_config(config)
    ap.add_argument('-d', '--date', dest='date', help='The query start date')
    return ap.parse_known_args()


def main():
    solr_endpoint = 'http://search-s9:8983/solr/main/select'

    args, extras = get_args()

    # Read the last indexed date
    date = args.date
    if date is None:
        with open('/data/last_indexed.txt', 'r') as f:
            date = f.read().strip()

    params = {
        'q': 'lang:en AND iscontent:true AND indexed:[%sZ TO NOW]' % date,
        'fl': 'wid,wikipages',
        'rows': '9999999',  # 10000
        'facet': 'true',
        'facet.limit': '-1',  # -1
        'facet.field': 'wid',
        'wt': 'json'
    }

    # Query Solr
    print 'Querying Solr...'
    while True:
        try:
            r = requests.get(solr_endpoint, params=params).json()
        except requests.exceptions.ConnectionError as e:
            print e
            print 'Connection error, retrying...'
            continue
        break

    docs = r['response']['docs']
    print '%d docs total' % len(docs)

    # Populate dict - {wid: number of articles}
    # (Using wikipages to approximate & # avoid multiple queries)
    print 'Populating article count dict...'
    articles = dict((doc['wid'], doc.get('wikipages', 0)) for doc in docs)

    # Populate dict - {wid: int indicating whether indexed since given date}
    ids = r['facet_counts']['facet_fields']['wid']
    print 'Populating indexed dict...'
    d = dict((int(ids[i]), ids[i+1]) for i in range(0, len(ids), 2))

    # Launch EC2 instances with appropriate shell scripts
    wids = filter(lambda x: d[x] > 0, d.keys())
    callback = lambda x: articles.get(x, 0)
    num_instances = config['max_size']
    user_data = """#!/bin/sh
cd /home/ubuntu/data-science-toolkit
git fetch origin
git checkout {git_ref}
git pull origin {git_ref} && sudo python setup.py install
python -m wikia_dstk.pipeline.wiki_data_extraction.run --s3path={{key}} {argstring} > /home/ubuntu/wiki_data_extraction.log""".format(git_ref=args.git_ref, argstring=argstring_from_namespace(args, extras))

    instances = run_instances_lb(
        wids, callback, num_instances, user_data, config)
    instance_ids = [i for i in instances.get()]
    conn = connect_to_region('us-west-2')
    print 'The following instances have been launched: %s' % str(instance_ids)
    conn.create_tags(instance_ids, {'Name': args.tag})


if __name__ == '__main__':
    main()
