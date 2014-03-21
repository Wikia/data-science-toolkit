from neo4jrestclient.client import GraphDatabase
from argparse import ArgumentParser
import requests


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'--graph-db', dest=u'graph_db', default=u'http://nlp-s3:7474/')
    ap.add_argument(u'--solr', dest=u'solr', default=u'http://search-s10:8983/solr/main')
    return ap.parse_args()


def infobox2dict(infoboxitems):
    dct = {}
    for line in infoboxitems:
        splt = line.split(u'|')
        if len(splt) > 2:
            key = splt[1].lower().strip(u':')
            dct[key] = u'|'.join(splt[2:]).lower()
    return dct


def main():
    args = get_args()
    db = GraphDatabase(args.graph_db)
    query_params = dict(q=u'iscontent:true AND lang:en AND infoboxes_txt:*', fl=u'id,title_en,infoboxes_txt,wid',
                        wt=u'json', start=0, rows=500)
    while True:
        response = requests.get(u'%s/select' % args.solr, params=query_params).json()

        for doc in response[u'response'][u'docs']:
            name = doc[u'title_en'].replace(u'"', u'')
            print name.encode('utf8')
            page_node = db.query(q=u"MATCH (x { name: '%s' })-[r]->(n) RETURN n" % name.replace(u"'", u"\\'"))
            dct = infobox2dict(doc[u'infoboxes_txt'])
            dct[u'ids'] = [doc[u'id']]
            if not page_node:
                dct[u'name'] = name
                page_node = db.nodes.create(**dct)
                page_node.labels.add(u'Page')
            else:
                for key in dct:
                    if key == u'name':
                        continue
                    page_node[key] = page_node.get(key, []) + dct[key]

            wiki_node = db.query(q=u"MATCH (x { wiki_id: '%d' })-[r]->(n) RETURN n" % doc['wid'])
            if not wiki_node:
                wiki_node = db.nodes.create(wiki_id=doc[u'wid'])
                wiki_node.labels.add(u'Wiki')
            db.relationships.create(wiki_node, u'involves', page_node)

        if response['response']['numFound'] <= query_params['start']:
            break
        query_params['start'] += query_params['rows']


if __name__ == '__main__':
    main()