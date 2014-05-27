import requests


def xml_to_exist(args, xml, wiki_id, page_id):
    """
    Sends xml to the desired exist-db endpoint
    :param args: an arg namespace -- allows flexible DI
    :type args:class:`argparse.Namespace`
    :param xml: the xml string
    :type xml: str
    :param wiki_id: the id of the wiki this page belongs to
    :type wiki_id: str
    :param page_id: the id of the page this is a parse of
    :type page_id: str
    """
    requests.put(u'%s/exist/%s/%s.xml' % (args.url, wiki_id, page_id),
                 data=xml, headers={u'Content-Type': u'application/xml', u'Content-Length': len(xml)})

