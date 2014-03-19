from boto import connect_s3
from argparse import ArgumentParser, FileType
import sys
import requests
import json
import os


def get_args():
    ap = ArgumentParser()
    ap.add_argument('--solr-url', dest='solr_url', default='http://dev-search:8983/solr/', help="The Solr endpoint")
    ap.add_argument('--solr-core', dest='solr_core', default='main', help="Which core in Solr to use")
    ap.add_argument('--infile', dest='infile', type=FileType('r'), help="A file on the local machine")
    ap.add_argument('--s3path', dest='s3path', help="All CSVs in a given folder, or a single CSV")
    ap.add_argument('--recommendations_field', dest='recommendations_field', default="recommendations_ss",
                    help="The field name to update in Solr")
    ap.add_argument('--batch-size', dest='batch_size', default=500, type=int,
                    help="Size of document batch to send to Solr")
    return ap.parse_args()


def get_s3_files(s3path):
    bucket = connect_s3().get_bucket('nlp-data')
    keys = bucket.get_all_keys(prefix=s3path)
    for key in keys:
        filename = key.name.split('/')[-1]
        key.get_contents_to_filename(filename)
        yield open(filename, 'r')


def send_solr_update(args, docs):
    solr_url = "%s/%s/update/" % (args.solr_url, args.solr_core)
    resp = requests.post(solr_url, data=json.dumps(docs), headers={'Content-type': 'application/json'})
    if resp.status_code != 200:
        raise Exception(resp.content)


def main():
    args = get_args()
    files = []
    if args.infile:
        files = [args.infile]
    elif args.s3path:
        files = get_s3_files(args.s3path)
    if not files:
        print "No Files!"
        sys.exit(1)
    for fl in files:
        print fl.name
        batch = []
        for line in fl:
            split = line.strip().split(',')
            batch.append({'id': split[0], args.recommendations_field: {'set': split[1:]}})
            if len(batch) == args.batch_size:
                send_solr_update(args, batch)
                batch = []
        if len(batch):
            send_solr_update(args, batch)
        fl.close()
        if args.s3path:
            os.remove(fl.name)

if __name__ == '__main__':
    main()