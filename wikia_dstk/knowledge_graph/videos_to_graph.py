from neo4jrestclient.client import GraphDatabase
from argparse import ArgumentParser
from multiprocessing import Pool
import traceback
import requests


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'--graph-db', dest=u'graph_db', default=u'http://nlp-s3:7474/')
    ap.add_argument(u'--solr', dest=u'solr', default=u'http://search-s10:8983/solr/main')
    ap.add_argument(u'--num-processes', dest=u'num_processes', type=int, default=8)
    return ap.parse_args()


def get_all_actors(args):
    query_params = dict(q=u'is_video:true AND video_actors_txt:*', fl=u'video_actors_txt',
                        wt=u'json', start=0, rows=500)
    actors = []
    while True:
        response = requests.get(u'%s/select' % args.solr, params=query_params).json()
        actors += [actor for doc in response[u'response'][u'docs'] for actor in doc.get(u'video_actors_txt', [])]
        if response[u'response'][u'numFound'] <= query_params[u'start']:
            return list(set(actors))
        query_params[u'start'] += query_params[u'rows']
        print query_params[u'start'], u"/", response[u'response'][u'numFound']


def main():
    args = get_args()
    db = GraphDatabase(args.graph_db)
    for label in [u'Video', u'Actor']:
        try:
            db.labels.create(label)
        except:
            continue

    print u"Getting all actors..."
    actors = get_all_actors(args)

    print u"Creating", len(actors), u"actor nodes"
    actor_nodes = []
    for i in range(0, len(actors), 500):
        print i
        actor_nodes += map(lambda x: db.nodes.create(name=x), actors[i:i+500])

    print u"Labeling actor nodes"
    map(lambda y: y.labels.add(u'Actor'), actor_nodes)
    actors_to_node = dict(zip(actors, actor_nodes))

    print u"Assigning videos to actors"
    query_params = dict(q=u'is_video:true AND video_actors_txt:*', fl=u'id,title_en,video_actors_txt,wid',
                        wt=u'json', start=0, rows=500)
    while True:
        response = requests.get(u'%s/select' % args.solr, params=query_params).json()
        for doc in response[u'response'][u'docs']:
            if u'title_en' not in doc:
                continue
            name = doc[u'title_en'].replace(u'"', u'').lower()

            print name.encode(u'utf8')
            video_node = db.nodes.create(doc_id=doc[u'id'], name=name.encode(u'utf8'))
            video_node.labels.add(u'Video')

            for actor in doc.get(u'video_actors_txt', []):
                actor_node = actors_to_node[actor]
                try:
                    db.relationships.create(video_node, u'stars', actor_node)
                    db.relationships.create(actor_node, u'acts_in', video_node)
                except Exception as e:
                    print e

        if response[u'response'][u'numFound'] <= query_params[u'start']:
            return True
        query_params[u'start'] += query_params[u'rows']




if __name__ == '__main__':
    main()