import os
import sys
import traceback
from boto import connect_s3

from nlp_services.caching import use_caching
from config import config

from nlp_services.discourse import AllEntitiesSentimentAndCountsService
from nlp_services.discourse.entities import TopEntitiesService, EntityDocumentCountsService, WpTopEntitiesService, WpEntityDocumentCountsService
from nlp_services.discourse.sentiment import WikiEntitySentimentService, WpWikiEntitySentimentService
from nlp_services.syntax import TopHeadsService

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
BUCKET = connect_s3().get_bucket('nlp-data')
SERVICES = config['services']

caching_dict = dict([(service+'.get', {'write_only': True}) for service in
                     SERVICES])
use_caching(per_service_cache=caching_dict)


def process_wiki(wid):
    print 'Calling wiki-level services on %s' % wid
    try:
        for service in SERVICES:
            try:
                print wid, service
                getattr(sys.modules[__name__], service)().get(wid)
                caching_dict[service+'.get'] = {'dont_compute': True}  # DRY!
                use_caching(per_service_cache=caching_dict)
            except KeyboardInterrupt:
                sys.exit()
            except:
                print 'Could not call %s on %s!' % (service, wid)
                print traceback.format_exc()
    except:
        print "Problem with", wid
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "".join(traceback.format_exception(exc_type, exc_value,
                                                 exc_traceback))

process_wiki(sys.argv[1])
