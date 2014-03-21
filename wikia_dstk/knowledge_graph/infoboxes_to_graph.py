from neo4jrestclient.client import GraphDatabase
from argparse import ArgumentParser
from multiprocessing import Pool
import requests


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'--graph-db', dest=u'graph_db', default=u'http://nlp-s3:7474/')
    ap.add_argument(u'--solr', dest=u'solr', default=u'http://search-s10:8983/solr/main')
    ap.add_argument(u'--num-processes', dest=u'num_processes', type=int, default=8)
    return ap.parse_args()


def escape_value(string):
    return string.replace(u"'", u"\\'")


def escape_key(string):
    return escape_value(string.replace(u" ", u"_"))


def handle_doc(tup):
    args, doc = tup
    db = GraphDatabase(args.graph_db)
    name = doc[u'title_en'].replace(u'"', u'')
    print name.encode(u'utf8')
    name_index = db.nodes.indexes.get('name')
    name_nodes = name_index[name]
    page_ids = [doc[u'id']]
    if not name_nodes:
        page_node = db.nodes.create(ids=page_ids, name=name)
        page_node.labels.add(u'Page')
    else:
        page_node = name_nodes[0]
        #page_node[u'ids'] = page_node[u'ids'] + page_ids

    box_nodes = []
    for line in doc[u'infoboxes_txt']:
        splt = line.split(u'|')
        if len(splt) > 2:
            key = splt[1].lower().strip(u':')
            value = u'|'.join(splt[2:]).lower()
            prop = name_index.get(value)
            if not prop:
                box_node = db.nodes.create(name=value)
                box_node.labels.add(u'Object')
            else:
                box_node = prop[0]
            print box_node
            db.relationships.create(box_node, 'is_%s_of' % key, page_node)
            page_node.labels.add(u'Subject')
            box_nodes.append(box_node)

    wiki_nodes = db.nodes.indexes.get('wiki_ids')[doc[u'wid']]
    if not wiki_nodes:
        wiki_node = db.nodes.create(wiki_id=doc[u'wid'])
        wiki_node.labels.add(u'Wiki')
    else:
        wiki_node = wiki_nodes[0]
    db.relationships.create(wiki_node, u'involves', page_node)
    for infobox_node in box_nodes:
        db.relationships.create(wiki_node, u'involves', infobox_node)


def run_queries(args, pool, start=0):
    query_params = dict(q=u'iscontent:true AND lang:en AND infoboxes_txt:*', fl=u'id,title_en,infoboxes_txt,wid',
                        wt=u'json', start=start, rows=500)
    response = requests.get(u'%s/select' % args.solr, params=query_params).json()

    map(handle_doc, [(args, doc) for doc in response[u'response'][u'docs']])
    if response['response']['numFound'] > query_params['start']:
        return run_queries(args, pool, start+query_params['rows'])
    return True


def main():
    args = get_args()
    db = GraphDatabase(args.graph_db)
    db.nodes.indexes.create('wiki_ids')
    db.nodes.indexes.create('name')
    pool = Pool(processes=args.num_processes)
    run_queries(args, pool)




if __name__ == '__main__':
    main()