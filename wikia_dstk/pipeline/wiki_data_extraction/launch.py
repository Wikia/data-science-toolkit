from __future__ import division
import requests
from wikia_dstk import get_argparser_from_config, argstring_from_namespace
from ...loadbalancing import run_instances_lb
from config import config


def get_args():
    ap = get_argparser_from_config(config)
    ap.add_argument('-d', '--date', dest='date', help='The query start date')
    return ap.parse_known_args()


def main():
    solr_endpoint = 'http://search-s10:8983/solr/main/select'

    args, extras = get_args()

    # Read the last indexed date
    date = args.date
    if date is None:
        with open('/data/last_indexed.txt', 'r') as f:
            date = f.read().strip()

    params = {
        'q': 'lang:en AND iscontent:true AND indexed:[%sZ TO NOW]' % date,
        'fl': 'wid,wikipages',
        'rows': '10000',  # 10000
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
    user_data = (
        "#!/bin/sh\n" +
        "/home/ubuntu/venv/bin/python -m " +
        "wikia_dstk.pipeline.wiki_data_extraction.run --s3path={key} " % (
            argstring_from_namespace(args, extras)) +
        "> /home/ubuntu/wiki_data_extraction.log")

    instances = run_instances_lb(
        wids, callback, num_instances, user_data, config)
    print 'The following instances have been launched: %s' % str(instances)


if __name__ == '__main__':
    main()
