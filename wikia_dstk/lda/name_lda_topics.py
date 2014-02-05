from __future__ import division

# Attempt to make sense of topic features by identifying the top entities
# common to them across different wikis, calculating Jaccard distance against
# top entities per wiki, and naming the topic feature after the best-fit wiki

import json
import logging
import requests
import sys
import traceback
from collections import defaultdict
from identify_wiki_subjects import identify_subject
from multiprocessing import Pool
from nlp_client.caching import useCaching
from nlp_client.services import TopEntitiesService
from wiki_recommender import as_euclidean, get_topics_sorted_keys

# Specify how many of the top wikis to iterate over
top_n = int(sys.argv[1])

SOLR_URL = 'http://dev-search.prod.wikia.net:8983/solr/xwiki/select'

useCaching(dontCompute=True)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
fh = logging.FileHandler('name_lda_topics.log')
fh.setLevel(logging.ERROR)
log.addHandler(fh)
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
log.addHandler(sh)

# Jaccard functions taken from https://github.com/mouradmourafiq/data-analysis
def jaccard_sim(tup_1, tup_2, verbose=False):
    """Calculate the Jaccard similiarity of 2 tuples"""
    sum = len(tup_1) + len(tup_2)
    set_1 = set(tup_1)
    set_2 = set(tup_2)
    inter = 0
    for i in (set_1 & set_2):
        count_1 = tup_1.count(i)
        count_2 = tup_2.count(i)
        inter += count_1 if count_1 < count_2 else count_2
    j_sim = inter/sum
    if verbose : print j_sim
    return j_sim

def jaccard_distance(tup_1, tup_2):
    """Calculate the Jaccard distance between 2 tuples"""
    return 1 - jaccard_sim(tup_1, tup_2)

def get_entities(wid):
    """Given a wiki id, return a list of the wiki's top entities"""
    params = {'q': 'id:%s' % wid, 'fl': 'entities_txt', 'wt': 'json'}
    j = requests.get(SOLR_URL, params=params).json()
    return j['response']['docs'][0].get('entities_txt', [])

def magic(wid):
    """Given a wiki id, return a tuple containing:
    0. The wiki id itself
    1. A list of topics for which term frequency is non-zero
    2. A list of top entities from the given wiki id
    3. A dict containing top entities from given & related wikis + their counts
    """
    log.info('Performing magic on ' + wid)
    wiki_entities = get_entities(wid)
    # Get related wikis, sorted by closest Euclidean distance
    doc, docs = as_euclidean(wid)
    related_wids = [wiki['id'] for wiki in docs]
    # Get a cumulative list of top entities from given wiki + all related wikis
    total_entities = wiki_entities[:]
    for related_wid in related_wids:
        total_entities.extend(get_entities(related_wid))
    # Get topics for which term frequency is non-zero
    topics = get_topics_sorted_keys(doc)
    # Create a dict with entity strings as keys, and counts as values
    tally = dict(zip(total_entities, map(total_entities.count, total_entities)))
    log.debug('For wid %s:\nTopics:%s\nWiki entities:%s\nTally:%s' %
              (wid, topics, wiki_entities, tally))
    return (wid, topics, wiki_entities, tally)

def name_topic(topic):
    """Given a topic, return the best-fit title based on Jaccard distance from
    a wiki's top entities"""
    log.info('Finding title for ' + topic)
    # Make sure topic is in dictionary
    if not entity_counts_for_topic.get(topic, False):
        return (topic, '')
    # Get top 50 entities associated with topic
    s = sorted(entity_counts_for_topic[topic].items(), key=lambda x: x[1],
               reverse=True)
    topic_entities = [k for (k, v) in s[:50]]
    # Find all wikis containing topic entities
    wids = []
    for topic_entity in topic_entities:
        wids.extend(wikis_for_entity.get(topic_entity, []))
    wids = list(set(wids))
    # Compute Jaccard distance 
    distances = [(wid, jaccard_distance(topic_entities, entities_for_wiki[wid]))
                 for wid in wids]
    best_wid, best_distance = min(distances, key=lambda x: x[1])
    log.debug('Jaccard distances: %s\nClosest: %s' % (distances, best_wid))
    title = identify_subject(best_wid, terms_only=True)
    return (topic, title)

# Instantiate global dictionaries
entity_counts_for_topic = defaultdict(lambda: defaultdict(int))
entities_for_wiki = defaultdict(list)
wikis_for_entity = defaultdict(list)

# Iterate over top 5k wikis
wids = [line.strip() for line in open('topwams.txt').readlines()[:top_n]]
for (wid, topics, wiki_entities, tally) in Pool(processes=8).map(magic, wids):
    for entity in tally:
        for topic in topics:
            # Keep tally of entity counts per topic
            entity_counts_for_topic[topic][entity] += tally[entity]
        # Keep track of wikis that entity appeared in
        wikis_for_entity[entity].append(wid)
    # Keep track of top entities present per wiki
    entities_for_wiki[wid] = wiki_entities

# Serialize dictionaries to avoid data loss upon Exception
with open('entity_counts_for_topic_%d.json' % top_n, 'w') as a:
    a.write(json.dumps(entity_counts_for_topic))
with open('entities_for_wiki_%d.json' % top_n, 'w') as b:
    b.write(json.dumps(entities_for_wiki))
with open('wikis_for_entity_%d.json' % top_n, 'w') as c:
    c.write(json.dumps(wikis_for_entity))

# Write best-fit title per topic feature to CSV
with open('topic_names_%d_wikis.csv' % top_n, 'w') as f:
    for (topic, title) in Pool(processes=8).map(name_topic,
                                                entity_counts_for_topic.keys()):
        try:
            f.write('%s,%s\n'.encode('utf-8') % (topic, title))
        except:
            log.error('%s: %s' % (topic, traceback.format_exc()))
