from argparse import ArgumentParser
from neo4jrestclient.client import GraphDatabase
from lxml import etree
import traceback
import requests


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'-d', u'--exist-db', dest=u'exist_db', default=u'http://nlp-s3:8080')
    ap.add_argument(u'-j', u'--neo4j', dest=u'neo4j', default=u'http://nlp-s3:7474')
    ap.add_argument(u'-u', u'--base-uri', dest=u'base_uri', required=True)
    return ap.parse_known_args()


def node_from_index(db, wiki_id, doc, sentence, sentence_index, word_xml):
    try:
        doc_sent_id = u"_".join([wiki_id, doc, sentence])
        word_id = int(word_xml.get(u'idx'))
        word = word_xml.text.encode(u'utf8')
        word_nodes = [node for node in sentence_index[doc_sent_id][word_id]]
        if not word_nodes:
            params = dict(word=word, doc=doc, sentence=sentence, word_id=word_id, wiki_id=wiki_id)
            word_node = db.nodes.create(**params)
            word_node.labels.add(u'Word')
            sentence_index[doc_sent_id][word_id] = word_node
        else:
            word_node = word_nodes[0]
        return word_node
    except (Exception, KeyError) as e:
        print e, traceback.format_exc()
        raise e


def get_query(base_uri):
    return u"""<query xmlns="http://exist.sourceforge.net/NS/exist"><text>
xquery version "3.0";
let $document := doc("%s")
return &lt;document base-uri="%s"&gt;{
for $dependencies in $document//dependencies[@type='collapsed-ccprocessed-dependencies']
    for $dependency in $dependencies
        return &lt;dependencywrapper base-uri="%s" sentence="{$dependency/../@id}"&gt;
                {$dependency}
               &lt;/dependencywrapper&gt;
}&lt;/document&gt;
</text></query>""" % (base_uri, base_uri, base_uri)


def main():
    args, _ = get_args()
    try:
        db = GraphDatabase(args.neo4j)
        sentence_index = db.nodes.indexes.get(u'sentence')

        r = requests.post(u'%s/exist/rest/db/' % args.exist_db,
                          data=get_query(args.base_uri),
                          headers={u'Content-type': u'application/xml'})

        document = etree.fromstring(r.content)
        wiki_id = args.base_uri.split(u'/')[-2]
        doc_id = args.base_uri.split(u'/')[-1].split(u'.')[0]
        for wrapper in document[0]:
            sentence = wrapper.get(u'sentence')
            for dependency in wrapper[0]:
                try:
                    governor = node_from_index(db, wiki_id, doc_id, sentence, sentence_index, dependency[0])
                    dependent = node_from_index(db, wiki_id, doc_id, sentence, sentence_index, dependency[1])
                    db.relationships.create(governor, dependency.get(u'type'), dependent)
                except IndexError:
                    continue
    except Exception as e:
        print e, traceback.format_exc()
        raise e


if __name__ == u'__main__':
    main()