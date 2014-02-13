import json
import os
import random
import re
import sys
import time
import traceback
from boto import connect_s3
from boto.exception import S3ResponseError
from boto.s3.key import Key
from boto.utils import get_instance_metadata
from multiprocessing import Pool

from nlp_services.caching import use_caching
from config import *

BUCKET = connect_s3().get_bucket('nlp-data')

# Get absolute path
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

# Load serialized services into memory
with open(os.path.join(BASE_PATH, 'config/services-config.json')) as f:
    SERVICES = json.loads(f.read())['services']

use_caching(per_service_cache=dict([(service+'.get', {'write_only': True}) for service in SERVICES]))

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
        #try:
        #    getattr(sys.modules[__name__], service)().get(doc_id)
        #except KeyboardInterrupt:
        #    sys.exit()
        #except Exception as e:
        #    print 'Could not call %s on %s!' % (service, doc_id)
        #    print traceback.format_exc()

    # write events to a new file
    wiki_event = Key(BUCKET)
    wiki_event.key = 'wiki_data_events/%s' % wiki_id
    wiki_event.set_contents_from_string(wiki_id)


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
