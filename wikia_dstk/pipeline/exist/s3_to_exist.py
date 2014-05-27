from boto import connect_s3
from argparse import ArgumentParser, FileType
from . import xml_to_exist


total_documents = 0


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'--wid', dest=u'wid', help=u'Control for a single wiki ID')
    ap.add_argument(u'--infile', dest=u'infile', type=FileType(u'r'), help=u'Control for multiple wiki IDs')
    ap.add_argument(u'--url', dest=u'url', default=u'http://localhost:8080', help=u'The exist DB URL')
    ap.add_argument(u'--user', dest=u'user', default='admin', help=u'Username to pass to exist')
    ap.add_argument(u'--password', dest=u'password', default='admin', help=u'Password to pass to exist')
    return ap.parse_args()


def for_wid(args, wid):
    """
    Suck down all xml parses from S3 for a wiki ID and put into exist
    """
    print u"Working on", wid
    global total_documents
    bucket = connect_s3().get_bucket(u'nlp-data')
    for key in bucket.list(prefix=u'xml/%s/' % wid):
        wiki_id, page_id = key.key.split(u'.')[0].split(u'/')[-2:]
        xml_to_exist(args, key.get_contents_as_string(), wiki_id, page_id)
        total_documents += 1
        if total_documents % 100 == 0:
            print total_documents


def main():
    args = get_args()
    if args.infile:
        wids = [line.strip() for line in args.infile]
        [for_wid(args, wid) for wid in wids]
    elif args.wid:
        for_wid(args, args.wid)


if __name__ == u'__main__':
    main()