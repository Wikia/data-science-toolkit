import requests
from lxml import etree
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient.request import NotFoundError
from argparse import ArgumentParser


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'-d', u'--exist-db', dest=u'exist_db', default=u'http://nlp-s3:8080')
    ap.add_argument(u'-j', u'--neo4j', dest=u'neo4j', default=u'http://nlp-s3:7474')
    return ap.parse_args()


def get_query(offset=1, limit=500):
    return u"""<query xmlns="http://exist.sourceforge.net/NS/exist" start="%d" max="%d"><text>xquery version "3.0";

let $copular-phrases :=
    for $sent in collection("/db/nlp/")//sentences/sentence
    where count($sent/dependencies[@type='collapsed-ccprocessed-dependencies']/dep[@type="cop"]) > 0
    return $sent/dependencies[@type='collapsed-ccprocessed-dependencies']

return $copular-phrases</text></query>""" % (offset, limit)


def node_from_index(db, index, word):
    word_nodes = [node for node in index[u'word'][word.encode(u'utf8')]]
    if not word_nodes:
        word_node = db.nodes.create(word=word)
        word_node.labels.add(u'Word')
        index[u'word'][word.encode(u'utf8')] = word_node
    else:
        word_node = word_nodes[0]
    return word_node


def main():
    args = get_args()
    offset = 1
    limit = 500
    db = GraphDatabase(args.neo4j)
    try:
        word_index = db.nodes.indexes.get(u'word')
    except NotFoundError:
        word_index = db.nodes.indexes.create(u'word')

    try:
        db.labels.create('Word')
    except:
        pass
    while True:
        r = requests.post(u'%s/exist/rest/db/' % args.exist_db,
                          data=get_query(offset, limit),
                          headers={u'Content-type': u'application/xml'})
        dom = etree.fromstring(r.content)
        for dependencies in dom:
            for dependency in dependencies:
                governor = node_from_index(db, word_index, dependency[0].text)
                dependent = node_from_index(db, word_index, dependency[1].text)
                db.relationships.create(governor, dependency.get('type'), dependent)

        hits = dom.get('{http://exist.sourceforge.net/NS/exist}hits')
        if not hits:
            print r.content
            break

        if int(hits) < offset:
            break
        offset += limit


if __name__ == '__main__':
    main()
