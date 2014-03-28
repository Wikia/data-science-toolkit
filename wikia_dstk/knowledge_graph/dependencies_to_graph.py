from .. import argstring_from_namespace
import requests
import traceback
from lxml import etree
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient.request import NotFoundError
from argparse import ArgumentParser, Namespace
from subprocess import Popen


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'-d', u'--exist-db', dest=u'exist_db', default=u'http://nlp-s3:8080')
    ap.add_argument(u'-j', u'--neo4j', dest=u'neo4j', default=u'http://nlp-s3:7474')
    ap.add_argument(u'-w', u'--wiki-id', dest=u'wiki_id', required=True)
    ap.add_argument(u'-n', u'--num-processes', dest=u'num_processes', default=6, type=int)
    return ap.parse_args()


def get_query(wid, offset=1, limit=500):
    return u"""<query xmlns="http://exist.sourceforge.net/NS/exist" start="%d" max="%d"><text>
xquery version "3.0";
let $documents := collection("/db/%s/")
for $document in $documents
    return &lt;document base-uri="{fn:base-uri($document)}" /&gt;
</text></query>""" % (offset, limit, wid)


def main():
    args = get_args()
    offset = 1
    limit = 500
    db = GraphDatabase(args.neo4j)

    try:
        db.labels.create('Word')
    except:
        pass

    try:
        db.nodes.indexes.get(u'sentence')
    except NotFoundError:
        db.nodes.indexes.create(u'sentence')

    try:
        db.nodes.indexes.get(u'wiki_word')
    except NotFoundError:
        db.nodes.indexes.create(u'wiki_word')

    while True:
        r = requests.post(u'%s/exist/rest/db/' % args.exist_db,
                          data=get_query(args.wiki_id, offset, limit),
                          headers={u'Content-type': u'application/xml'})
        dom = etree.fromstring(r.content)

        dom_args = [Namespace(base_uri=d.get(u'base-uri'), **vars(args)) for d in dom]
        processes = []
        while len(dom_args):
            while len(processes) < args.num_processes and len(dom_args):
                this_args = dom_args.pop()
                cmd = (u'/usr/bin/python -m wikia_dstk.knowledge_graph.dependencies_to_graph_worker '
                       + argstring_from_namespace(this_args))
                processes.append(Popen(cmd, shell=True))
            processes = filter(lambda x: x.poll() is None, processes)

        hits = dom.get(u'{http://exist.sourceforge.net/NS/exist}hits')
        if not hits:
            print r.content
            break

        print offset, "/", hits
        offset += limit
        if int(hits) <= offset:
            break


if __name__ == u'__main__':
    main()
