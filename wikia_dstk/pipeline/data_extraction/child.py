import random
import re
import sys
import time
from boto import connect_s3
from boto.exception import S3ResponseError
from boto.s3.key import Key
from boto.utils import get_instance_metadata

from nlp_services.caching import use_caching
from .config import config

from nlp_services.discourse.entities import CoreferenceCountsService, EntityCountsService
from nlp_services.discourse.sentiment import DocumentSentimentService, DocumentEntitySentimentService, WpDocumentEntitySentimentService
from nlp_services.syntax import AllNounPhrasesService, AllVerbPhrasesService, HeadsService

BUCKET = connect_s3().get_bucket('nlp-data')
SERVICES = config['services']

use_caching(per_service_cache=dict([(service+'.get', {'write_only': True}) for
                                    service in SERVICES]))


def process_file(filename):
    if filename.strip() == '':
        return  # newline at end of file
    global SERVICES
    match = re.search('([0-9]+)/([0-9]+)', filename)
    if match is None:
        print "No match for %s" % filename
        return

    wiki_id = match.group(1)
    doc_id = '%s_%s' % (match.group(1), match.group(2))
    print 'Calling doc-level services on %s' % wiki_id
    for service in SERVICES:
        print wiki_id, service
        getattr(sys.modules[__name__], service)().get(doc_id)


def call_services(keyname):
    global BUCKET

    print keyname
    key = BUCKET.get_key(keyname)
    if key is None:
        print 'no key found'
        return

    eventfile = "data_processing/%s_%s_%s" % (get_instance_metadata()['local-hostname'], str(time.time()), str(int(random.randint(0, 100))))
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
    k = Key(BUCKET)
    k.key = eventfile

    print k.key
    map(process_file, k.get_contents_as_string().split('\n'))

    print 'EVENT FILE %s COMPLETE' % eventfile
    k.delete()


call_services(sys.argv[1])
