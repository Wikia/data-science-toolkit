from __future__ import division
import requests
from .. import EC2Connection
from optparse import OptionParser, OptionError

from config import config
from top5k import top5k

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

# Read the last indexed date
date = options.date
if date is None:
    with open(LAST_INDEXED, 'r') as f:
        date = f.read().strip()

params = {
             'q':'lang_s:en',
             'fl':'id,articles_i',
             'rows':'1000', # 10000
             'facet': 'true',
             'facet.limit': '100', # -1
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
wids = filter(lambda x: d.get(x), [l.strip() for l in open('top5k.txt')])

# TODO: Split into groups of approx equal total doc count
parts = config['max_size']
wids = sorted(wids, key=lambda x: articles.get(x, 0), reverse=True)
print wids; import sys; sys.exit(0)

# TODO: Write multiple user_data shell scripts with wids passed as args and launch the appropriate # of instances
