from boto import connect_s3
from argparse import ArgumentParser
from os import makedirs


def get_args():
    ap = ArgumentParser()
    ap.add_argument('-w', '--wiki-id', dest='wiki_id', required=True)
    ap.add_argument('-d', '--dest', dest='dest', default='/tmp/')
    return ap.parse_args()


def main():
    args = get_args()
    bucket = connect_s3().get_bucket("nlp-data")
    makedirs("%s/%s" % (args.dest, args.wiki_id))
    for key in bucket.list(prefix='xml/%s' % args.wiki_id):
        fname = "%s/%s/%s" % (args.dest, args.wiki_id, key.name.split('/')[-1])
        print key, '->', fname
        key.get_contents_to_filename(fname)


if __name__ == '__main__':
    main()