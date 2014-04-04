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


def escape_value(string):
    return string.replace(u"'", u"\\'")


def handle_doc(tup):
    try:
        args, doc = tup
        db = GraphDatabase(args.graph_db)
        name = doc[u'title_en'].replace(u'"', u'').lower()
        print name.encode(u'utf8')
        video_index = db.nodes.indexes.get(u'video')
        actor_index = db.nodes.indexes.get(u'actor')
        wid = doc[u'wid']
        video_nodes = [node for node in video_index[wid][name.encode(u'utf8')]]
        if not video_nodes:
            video_node = db.nodes.create(ids=doc[u'id'], name=name.encode(u'utf8'))
            video_node.labels.add(u'Video')
            video_index[wid][name.encode(u'utf8')] = video_node
        else:
            video_node = video_nodes[0]

        for actor in doc[u'video_actors_txt']:
            actors = [node for node in actor_index[wid][actor]]
            if not actors:
                actor_node = db.nodes.create(name=actor)
                if u"Actor" not in actor_node.labels:
                    actor_node.labels.add(u'Actor')
                actor_index[doc[u'wid']][actor] = actor_node
            else:
                actor_node = actors[0]

            try:
                db.relationships.create(video_node, u'stars', actor_node)
                db.relationships.create(actor_node, u'acts_in', video_node)
            except Exception as e:
                print e
    except Exception as e:
        print e
        traceback.format_exc()
        raise e


def run_queries(args, pool, start=0):
    query_params = dict(q=u'is_video:true AND video_actors_txt:*', fl=u'id,title_en,video_actors_txt,wid',
                        wt=u'json', start=start, rows=500)
    while True:
        response = requests.get(u'%s/select' % args.solr, params=query_params).json()
        pool.map(handle_doc, [(args, doc) for doc in response[u'response'][u'docs']])
        if response[u'response'][u'numFound'] <= query_params[u'start']:
            return True
        query_params['start'] += query_params['rows']


def main():
    args = get_args()
    db = GraphDatabase(args.graph_db)
    try:
        video_index = db.nodes.indexes.create(u'video')
    except:
        pass
    try:
        actor_index = db.nodes.indexes.create(u'actor')
    except:
        pass
    for label in [u'Video', u'Actor']:
        try:
            db.labels.create(label)
        except:
            continue
    pool = Pool(processes=args.num_processes)
    run_queries(args, pool)


if __name__ == '__main__':
    main()