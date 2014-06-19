from __future__ import division
import json
import requests
from boto.ec2 import connect_to_region
from wikia_dstk import get_argparser_from_config, argstring_from_namespace
from ...loadbalancing import run_instances_lb
from config import config


def get_args():
    ap = get_argparser_from_config(config)
    ap.add_argument('-d', '--date', dest='date', help='The query start date')
    ap.add_argument('-D', '--dump-json', dest='dump_json',
                    help='Dump Solr data to JSON')
    ap.add_argument('-L', '--load-json', dest='load_json',
                    help='Load Solr data from JSON')
    ap.add_argument('-e', '--solr-endpoint', dest='solr_endpoint', default='http://search-s9:8983/solr',
                    help="The Solr endpoint")
    ap.add_argument('-a', '--all', dest='all', action='store_true', default=False,
                    help="Index all wikis")
    return ap.parse_known_args()


def execute_old(args):
    if args.load_json is not None:
        with open(args.load_json) as load:
            r = json.loads(load.read())
    else:
        # Read the last indexed date
        date = args.date
        if date is None:
            with open('/data/last_indexed.txt', 'r') as f:
                date = f.read().strip()

        params = {
            'q': 'lang:en AND iscontent:true AND indexed:[%sZ TO NOW]' % date,
            'fl': 'wid,wikipages',
            'rows': '9999999',
            'facet': 'true',
            'facet.limit': '-1',  # -1
            'facet.field': 'wid',
            'wt': 'json'
        }

        # Query Solr
        print 'Querying Solr...'
        while True:
            try:
                r = requests.get("%s/main/select", args.solr_endpoint, params=params).json()
            except requests.exceptions.ConnectionError as e:
                print e
                print 'Connection error, retrying...'
                continue
            break

    if args.dump_json is not None:
        with open(args.dump_json, 'w') as dump:
            dump.write(json.dumps(r))

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
    wids = filter(lambda x: d[x] > 0, d.keys())
    return articles, wids


def execute_all(args):
    params = dict(wt='json', q='lang_s:en AND articles_i:[50 TO *]', rows=500, start=0, fields='articles_i, id')
    return_data = []
    while True:
        response = requests.get('%s/xwiki/select' % args.solr_endpoint, params=params).json()
        return_data += [(doc['id'], doc['articles_i']) for doc in response['response']['docs']]
        if response['response']['numFound'] <= params['start'] + params['rows']:
            return_dict = dict(return_data)
            return return_dict, return_dict.values()
        params['start'] += params['rows']


def main():
    args, extras = get_args()

    if args.all:
        articles, wids = execute_all(args)
    else:
        articles, wids = execute_old(args)

    # Launch EC2 instances with appropriate shell scripts
    callback = lambda x: articles.get(x, 0)
    num_instances = config['max_size']
    user_data = """#!/bin/sh
cd /home/ubuntu/nlp_services
git fetch origin
git checkout master
git pull origin master && sudo python setup.py install
cd /home/ubuntu/data-science-toolkit
git fetch origin
git checkout {git_ref}
git pull origin {git_ref} && sudo python setup.py install
cd /home/ubuntu
python -m wikia_dstk.pipeline.wiki_data_extraction.run --s3path={{key}} {argstring} 2>&1 | tee -a /home/ubuntu/wiki_data_extraction.log""".format(git_ref=args.git_ref, argstring=argstring_from_namespace(args, extras))
#python -m wikia_dstk.pipeline.wiki_data_extraction.test_log &> /home/ubuntu/test.log""".format(git_ref=args.git_ref)
    instances = run_instances_lb(
        wids, callback, num_instances, user_data, config)
    instance_ids = [i for i in instances.get() for i in i]
    conn = connect_to_region('us-west-2')
    conn.create_tags(instance_ids, {'Name': args.tag, 'type': 'wiki_data_extraction'})
    print 'The following instances have been launched: %s' % str(instance_ids)


if __name__ == '__main__':
    main()
