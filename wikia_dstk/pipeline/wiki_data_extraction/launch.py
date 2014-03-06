from __future__ import division
import requests
from ... import run_instances_lb
from optparse import OptionParser

from config import config

LAST_INDEXED = '/data/last_indexed.txt'
SOLR = 'http://search-s10:8983/solr/main/select'

op = OptionParser()
op.add_option('-d', '--date', dest='date', help='The query start date')
op.add_option('-r', '--region', dest='region',
              help='The EC2 region to connect to')
op.add_option('-c', '--cost', dest='price', help='The maximum bid price')
op.add_option('-a', '--ami', dest='ami', help='The AMI to use')
op.add_option('-k', '--key', dest='key', help='The name of the key pair')
op.add_option('-s', '--security-groups', dest='sec',
              help='The security groups with which to associate instances')
op.add_option('-i', '--instance-type', dest='type',
              help='The type of instance to run')
op.add_option('-t', '--tag', dest='tag',
              help='The name of the tag to operate over')
op.add_option('-m', '--max-size', dest='max_size', type='int',
              help='The maximum allowable number of simultaneous instances')
(options, args) = op.parse_args()

config.update([(k, v) for (k, v) in vars(options).items() if v is not None])

# Read the last indexed date
date = options.date
if date is None:
    with open(LAST_INDEXED, 'r') as f:
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
        r = requests.get(SOLR, params=params).json()
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
callable = lambda x: articles.get(x, 0)
num_instances = config['max_size']
user_data = """#!/bin/sh
/home/ubuntu/venv/bin/python -m wikia_dstk.pipeline.wiki_data_extraction.run -w %s > /home/ubuntu/wiki_data_extraction.log
"""
instances = run_instances_lb(wids, callable, num_instances, user_data, config)
print 'The following instances have been launched: %s' % str(instances)

## DEBUG
#
#from ... import EC2Connection
#conn = EC2Connection(config)
#
#
#def dns(n):
#    print conn.conn.get_only_instances(instances)[n].public_dns_name
#
#
#def output(n):
#    print conn.conn.get_console_output(instances[n]).output
