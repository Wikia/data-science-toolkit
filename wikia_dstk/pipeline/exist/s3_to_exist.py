import os
import shutil
import codecs
from subprocess import check_output
from boto import connect_s3
from argparse import ArgumentParser, FileType
from multiprocessing import Pool


total_documents = 0


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'--wid', dest=u'wid', help=u'Control for a single wiki ID')
    ap.add_argument(u'--infile', dest=u'infile', type=FileType(u'r'), help=u'Control for multiple wiki IDs')
    ap.add_argument(u'--url', dest=u'url', default=u'http://localhost:8080', help=u'The exist DB URL')
    ap.add_argument(u'--user', dest=u'user', default='admin', help=u'Username to pass to exist')
    ap.add_argument(u'--password', dest=u'password', default='', help=u'Password to pass to exist')
    ap.add_argument(u'--threads', dest=u'threads', default=8, type=int)
    ap.add_argument(u'--exist-path', dest=u'exist_path', default=u'/opt/exist/')
    return ap.parse_args()


def key_to_file(key):
    """
    Send a given key's contents to exist
    """
    wiki_id, page_id = key.key.split(u'.')[0].split(u'/')[-2:]
    with codecs.open(u'/tmp/%s/%s.xml' % (wiki_id, page_id), u'w') as fl:
        key.get_contents_to_file(fl)


def for_wid(args, wid):
    """
    Suck down all xml parses from S3 for a wiki ID and put into exist
    """
    print u"Working on", wid
    wid_path = u'/tmp/%s' % wid
    try:
        os.mkdir(wid_path)
    except OSError:
        pass
    bucket = connect_s3().get_bucket(u'nlp-data')
    pool = Pool(processes=args.threads)
    pool.map_async(key_to_file, bucket.list(prefix=u'xml/%s/' % wid)).get()
    print check_output([args.exist_path+u'/bin/client.sh', u'-m', u'/db/nlp/%s' % wid, u'-p', u'/filesystem-path'])
    shutil.rmtree(wid_path)


def main():
    args = get_args()
    if args.infile:
        wids = [line.strip() for line in args.infile]
        [for_wid(args, wid) for wid in wids]
    elif args.wid:
        for_wid(args, args.wid)


if __name__ == u'__main__':
    main()