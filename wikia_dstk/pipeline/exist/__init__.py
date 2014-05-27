import httplib


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

    con = httplib.HTTP(args.url.replace('http://', ''))
    con.putrequest('PUT', '/exist/nlp/%s/%s' % (wiki_id, page_id))
    con.putheader('Content-Type', 'application/xml')
    con.putheader('Content-Length', '%d' % len(xml))
    con.endheaders()
    con.send(xml)
    errcode, errmsg, headers = con.getreply()
    if errcode != 200:
        f = con.getfile()
        print 'An error occurred: %s' % errmsg
        f.close()
    else:
        print "Ok."