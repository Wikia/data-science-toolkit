import random
import re
import sys
import time
from boto import connect_s3
from boto.exception import S3ResponseError
from boto.s3.key import Key
from boto.utils import get_instance_metadata

from nlp_services.caching import use_caching
from ... import get_argparser_from_config

from nlp_services.discourse.entities import *
from nlp_services.discourse.sentiment import *
from nlp_services.authority import *
from nlp_services.syntax import *


def process_file(filename, services):
    if filename.strip() == '':
        return  # newline at end of file

    match = re.search('([0-9]+)/([0-9]+)', filename)
    if match is None:
        print "No match for %s" % filename
        return

    wiki_id = match.group(1)
    doc_id = '%s_%s' % (match.group(1), match.group(2))
    print 'Calling doc-level services on %s' % wiki_id
    for service in services:
        print wiki_id, service
        getattr(sys.modules[__name__], service)().get(doc_id)


def call_services(args):
    bucket = connect_s3().get_bucket('nlp-data')
    print args.key
    key = bucket.get_key(args.key)
    if key is None:
        print 'no key found'
        return

    eventfile = "data_processing/%s_%s_%s" % (
        get_instance_metadata()['local-hostname'], str(time.time()),
        str(int(random.randint(0, 100))))
    try:
        key.copy('nlp-data', eventfile)
        key.delete()
    except S3ResponseError as e:
        print e
        print 'EVENT FILE %s NOT FOUND!' % eventfile
        return
    except KeyboardInterrupt:
        sys.exit()

    print 'STARTING EVENT FILE %s' % eventfile
    k = Key(bucket)
    k.key = eventfile

    print k.key
    map(lambda x: process_file(x, args.services.split(',')), k.get_contents_as_string().split('\n'))

    print 'EVENT FILE %s COMPLETE' % eventfile
    k.delete()


def get_args():
    ap = get_argparser_from_config()
    ap.add_argument('--key', dest='key', required=True)
    return ap.parse_known_args()


def main():
    args, _ = get_args()
    use_caching(per_service_cache=dict([(service+'.get', {'write_only': True}) for
                                        service in args.services.split(',')]))
    call_services(args)


if __name__ == '__main__':
    main()