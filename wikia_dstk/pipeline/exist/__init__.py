import requests
from requests.auth import HTTPBasicAuth


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
    :return: true if worked, false if not
    :rtype: bool
    """
    r = requests.put('%s/exist/rest/nlp/%s/%s.xml' % (args.url, wiki_id, page_id),
                     data=str(xml),
                     headers={'Content-Type': 'application/xml', 'Content-Length': len(xml), 'Charset': 'utf-8'},
                     auth=HTTPBasicAuth(args.user, args.password))
    if r.status_code > 299:
        print r.content, r.url, r.status_code
        return False
    return True


def delete_collection(args, wiki_id):
    """
    Deletes a given collection -- usually used when we're going to do a full reindex
    Please keep in mind tht this means that that data will be unavailable until reindex is complete
    :param args: an arg namespace -- allows flexible DI
    :type args:class:`argparse.Namespace`
    :param wiki_id: the id of the wiki corresponding to that collection
    :type wiki_id: str
    :return: true if worked, false if not
    :rtype: bool
    """
    r = requests.delete('%s/exist/rest/nlp/%s/' % (args.url, wiki_id), auth=HTTPBasicAuth(args.user, args.password))
    if r.status_code > 299:
        print r.content, r.url, r.status_code
        return False
    return True