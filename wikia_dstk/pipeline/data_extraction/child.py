import random
import re
import sys
import time
from boto import connect_s3
from boto.s3.key import Key
from boto.utils import get_instance_metadata
from multiprocessing import Pool

from nlp_services.caching import use_caching
from wikia_dstk import get_argparser_from_config
from config import default_config

from nlp_services.discourse.entities import *
from nlp_services.discourse.sentiment import *
from nlp_services.syntax import *

doc_id = None


def get_service(service):
    print doc_id, service
    return getattr(sys.modules[__name__], service)().get(doc_id)


def process_file(filename, services):
    global doc_id

    if filename.strip() == '':
        return  # newline at end of file

    match = re.search('([0-9]+)/([0-9]+)', filename)
    if match is None:
        print "No match for %s" % filename
        return

    wiki_id = match.group(1)
    doc_id = '%s_%s' % (match.group(1), match.group(2))
    print 'Calling doc-level services on %s' % doc_id

    pool = Pool(processes=8)
    s = pool.map_async(get_service, services)
    s.wait()


def call_services(args):
    bucket = connect_s3().get_bucket('nlp-data')
    key = bucket.get_key(args.s3key)
    if key is None:
        return

    folder = args.s3key.split('/')[0]

    eventfile = "%s_processing/%s_%s_%s" % (
        folder, get_instance_metadata()['local-hostname'], str(time.time()),
        str(int(random.randint(0, 100))))

    key.copy('nlp-data', eventfile)
    key.delete()

    k = Key(bucket)
    k.key = eventfile

    lines = k.get_contents_as_string().split('\n')
    map(lambda x: process_file(x, args.services.split(',')), lines)
    print args.s3key, len(lines), "ids completed"

    k.delete()


def get_args():
    ap = get_argparser_from_config(default_config)
    ap.add_argument('--s3key', dest='s3key', required=True)
    return ap.parse_known_args()


def main():
    args, _ = get_args()
    use_caching(per_service_cache=dict(
        [(service+'.get', {'write_only': True}) for service in
         args.services.split(',')]))
    call_services(args)


if __name__ == '__main__':
    main()
