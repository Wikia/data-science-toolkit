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
    name = doc[u'title_en'].replace(u'"', u'').lower()
    print name.encode(u'utf8')
    name_index = db.nodes.indexes.get(u'name')
    wiki_index = db.nodes.indexes.get(u'wiki_ids')
    name_nodes = [node for node in name_index[u'name'][name.encode('utf8')]]
    page_ids = [doc[u'id']]
    if not name_nodes:
        page_node = db.nodes.create(ids=page_ids, name=name.encode('utf8'))
        page_node.labels.add(u'Page')
        name_index[u'name'][name.encode('utf8')] = page_node
    else:
        page_node = name_nodes[0]
        if u'ids' in page_node:
            page_node[u'ids'] = list(set(page_node[u'ids'] + page_ids))
        else:
            page_node[u'ids'] = [page_ids]

    box_nodes = []
    for line in doc[u'infoboxes_txt']:
        splt = line.split(u'|')
        if len(splt) > 2:
            key = splt[1].lower().replace(u':', '').strip()
            value = u'|'.join(splt[2:]).strip().lower()
            props = [node for node in name_index[u'name'][value.encode('utf8')]]
            if not props:
                box_node = db.nodes.create(name=value)
                if u"Object" not in box_node.labels:
                    box_node.labels.add(u'Object')
                name_index[u'name'][value] = box_node
            else:
                box_node = props[0]
            try:
                db.relationships.create(box_node, (u'is_%s_of' % escape_key(key)).encode('utf8'), page_node)
            except Exception as e:
                print e
            if u"Subject" not in page_node.labels:
                page_node.labels.add(u'Subject')
            box_nodes.append(box_node)

    wiki_nodes = [node for node in wiki_index[u'wiki_id'][doc[u'wid']]]
    if not wiki_nodes:
        wiki_node = db.nodes.create(wiki_id=doc[u'wid'])
        wiki_node.labels.add(u'Wiki')
        wiki_index[u'wiki_id'][doc[u'wid']] = wiki_node
    else:
        wiki_node = wiki_nodes[0]
    try:
        db.relationships.create(wiki_node, u'involves', page_node)
    except Exception as e:
        print e
    for infobox_node in box_nodes:
        try:
            db.relationships.create(wiki_node, u'involves', infobox_node)
        except Exception as e:
            print e


def run_queries(args, pool, start=0):
    query_params = dict(q=u'iscontent:true AND lang:en AND infoboxes_txt:*', fl=u'id,title_en,infoboxes_txt,wid',
                        wt=u'json', start=start, rows=500)
    response = requests.get(u'%s/select' % args.solr, params=query_params).json()

    map(handle_doc, [(args, doc) for doc in response[u'response'][u'docs']])
    if response[u'response'][u'numFound'] > query_params[u'start']:
        return run_queries(args, pool, start+query_params[u'rows'])
    return True


def main():
    args = get_args()
    db = GraphDatabase(args.graph_db)
    for label in [u'Page', u'Object', u'Subject', u'Wiki']:
        try:
            db.labels.create(label)
        except:
            continue
    pool = Pool(processes=args.num_processes)
    run_queries(args, pool)


if __name__ == '__main__':
    main()