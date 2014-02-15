from __future__ import division
import requests
from .. import EC2Connection
from ..top5k import top5k
from collections import defaultdict
from optparse import OptionParser, OptionError

from config import config

LAST_INDEXED = '/data/last_indexed.txt'
SOLR = 'http://search-s10:8983/solr/xwiki/select'

op = OptionParser()
op.add_option('-d', '--date', dest='date', help='The query start date')
op.add_option('-r', '--region', dest='region', help='The EC2 region to connect to')
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

ec2_conn = EC2Connection(config)

# Read the last indexed date
date = options.date
if date is None:
    with open(LAST_INDEXED, 'r') as f:
        date = f.read().strip()

params = {
             'q':'lang_s:en',
             'fl':'id,articles_i',
             'rows':'2000', # 10000
             'sort': 'wam_i desc',
             'facet': 'true',
             'facet.limit': '500', # -1
             'facet.query': 'indexed:[%sZ TO NOW]' % date,
             'facet.field': 'id',
             'wt': 'json'
         }

# Populate dict - {wid: int indicating whether indexed since given date}
r = requests.get(SOLR, params=params).json()
docs = r['response']['docs']
articles = dict((doc['id'], doc.get('articles_i', 0)) for doc in docs)
#from pprint import pprint; pprint(articles); import sys; sys.exit(0)
ids = r['facet_counts']['facet_fields']['id']
d = dict((ids[i], ids[i+1]) for i in range(0, len(ids), 2))

# Find wikis in top 5k indexed since given date
wids = filter(lambda x: d.get(x), top5k)

# Split into groups of approx equal total doc count, args = {inst #: wids to pass}
parts = config['max_size']
wids = sorted(wids, key=lambda x: articles.get(x, 0), reverse=True)
args = defaultdict(list)
for i in range(0, len(wids), parts):
    for n, wid in enumerate(wids[i:i+parts]):
        args[n].append(wid)

# Write individual user_data shell scripts with wids passed as args and launch
# EC2 instances to execute them one at a time
for n in args:
    user_data = """#!/bin/sh
    /home/ubuntu/venv/bin/python -m wikia_dstk.pipeline.wiki_data_extraction.run %s
    """ % ','.join(args[n])
    ec2_conn.add_instances(1, user_data)
