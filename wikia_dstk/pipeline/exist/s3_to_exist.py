import os
import shutil
import codecs
from . import delete_collection, create_collection
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
    ap.add_argument(u'--no-delete', dest=u"delete_on_reindex", default=True, action=u"store_true")
    return ap.parse_args()


def key_to_file(key):
    """
    Send a given key's contents to exist
    """
    if key.size:
        wiki_id, page_id = key.key.split(u'.')[0].split(u'/')[-2:]
        with codecs.open(u'/tmp/%s/%s.xml' % (wiki_id, page_id), u'w') as fl:
            key.get_contents_to_file(fl)


def xquery_ingest_files(args, wiki_id):
    """
    Populates a given collection
    :param args: an arg namespace -- allows flexible DI
    :type args:class:`argparse.Namespace`
    :param wiki_id: the id of the wiki corresponding to that collection
    :type wiki_id: str
    :return: true if worked, false if not
    :rtype: bool
    """

    abs_dirname = '/tmp/%s/' % wiki_id
    files = [os.path.join(abs_dirname, f) for f in os.listdir(abs_dirname)]
    dirs = ['%s/%d' % (abs_dirname, i) for i in range(0, n)]
    map(os.mkdir, dirs)
    dirlen = len(dirs)
    [shutil.move(f, os.path.join(dirs[i % dirlen], os.path.basename(f))) for i, f in enumerate(files)]

    pool = Pool(processes=args.threads)
    calls = [("%s/bin/client.sh -m /db/nlp/%s -p %s" % (args.exist_path, wiki_id, tdir)).split(' ') for tdir in dirs]
    print pool.map(check_output, calls)


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
    current = len([f for f in os.listdir(wid_path) if os.path.isfile(wid_path+u'/'+f)])
    print u"Validating XML and removing cruft"
    check_output(u" | ".join([u"xmllint /tmp/%s/* --noout 2>&1" % wid, u"grep 'error'",
                              u"perl -pe 's/^([^:]*):.*$/\\1/g'", u"xargs sudo rm -f"]),
                 shell=True)
    to_index = len([f for f in os.listdir(wid_path) if os.path.isfile(wid_path+u'/'+f)])
    print u"Deleted %d invalid documents" % (current - to_index)
    if args.delete_on_reindex:
        print u"Deleting current collection for performance"
        delete_collection(args, wid)
    print u"Indexing %d documents" % to_index
    create_collection(args, wid)
    xquery_ingest_files(args, wid)


def main():
    args = get_args()
    if args.infile:
        wids = [line.strip() for line in args.infile]
        [for_wid(args, wid) for wid in wids]
    elif args.wid:
        for_wid(args, args.wid)


if __name__ == u'__main__':
    main()