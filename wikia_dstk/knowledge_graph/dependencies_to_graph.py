import requests
import traceback
from lxml import etree
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient.request import NotFoundError
from argparse import ArgumentParser, Namespace
from multiprocessing import Pool


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
    for $dependencies in $document//dependencies[@type='collapsed-ccprocessed-dependencies']
        for $dependency in $dependencies
            return &lt;dependencywrapper base-uri="{fn:base-uri($document)}" sentence="{$dependency/../@id}"&gt;
                    {$dependency}
                   &lt;/dependencywrapper&gt;
</text></query>""" % (offset, limit, wid)


def node_from_index(db, wiki_id, doc, sentence, word_xml):
    try:
        sentence_index = db.nodes.indexes.get(u'sentence')
        wiki_word_index = db.nodes.indexes.get(u'wiki_word')
        doc_sent_id = u"_".join([wiki_id, doc, sentence])
        word_id = word_xml.get(u'idx')
        word = word_xml.text.encode(u'utf8')
        word_nodes = [node for node in sentence_index[doc_sent_id][word_id]]
        if not word_nodes:
            word_node = db.nodes.create(word=word, doc=doc, sentence=sentence, word_id=word_id, wiki_id=wiki_id)
            word_node.labels.add(u'Word')
            sentence_index[doc_sent_id][word_id] = word_node
            words = wiki_word_index[wiki_id][word]
            if words:
                wiki_word_index[wiki_id][word] += [word_node]
            else:
                wiki_word_index[wiki_id][word] = [word_node]
        else:
            word_node = word_nodes[0]
        return word_node
    except Exception as e:
        print e, traceback.format_exc()
        raise e


def process_dependency(args):
    db = GraphDatabase(args.neo4j)
    doc = args.dependencies_wrapper.get(u'base-uri').split(u'/')[-1].split(u'.')[0]
    sentence = args.dependencies_wrapper.get(u'sentence')
    for dependency in args.dependencies_wrapper[0]:
        try:
            governor = node_from_index(db, args.wiki_id, doc, sentence, dependency[0])
            dependent = node_from_index(db, args.wiki_id, doc, sentence, dependency[1])
            db.relationships.create(governor, dependency.get(u'type'), dependent)
        except IndexError:
            continue


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

    p = Pool(processes=args.num_processes)
    while True:
        r = requests.post(u'%s/exist/rest/db/' % args.exist_db,
                          data=get_query(args.wiki_id, offset, limit),
                          headers={u'Content-type': u'application/xml'})
        dom = etree.fromstring(r.content)
        p.map_async(process_dependency, [Namespace(dependencies_wrapper=d, **vars(args)) for d in dom]).get()
        hits = dom.get(u'{http://exist.sourceforge.net/NS/exist}hits')
        if not hits:
            print r.content
            break

        if int(hits) <= offset:
            break
        offset += limit


if __name__ == u'__main__':
    main()
