import os
import sys
import traceback
from multiprocessing import Pool

from nlp_services.caching import use_caching
from config import config
from ... import log, get_argparser_from_config

# we dump everything in here to be dynamic
from nlp_services.discourse import *
from nlp_services.discourse.entities import *
from nlp_services.discourse.sentiment import *
from nlp_services.syntax import *
from nlp_services.title_confirmation import *
from nlp_services.authority import *

wiki_id = None


def get_args():
    ap = get_argparser_from_config(config)
    ap.add_argument('-w', '--wiki-id', dest='wiki_id', required=True,
                    help="The wiki ID to operate over")
    return ap.parse_known_args()


def get_service(service):
    log(wiki_id, service)
    try:
        getattr(sys.modules[__name__], service)().get(wiki_id)
    except:
        log(wiki_id, service, traceback.format_exc())


def main():
    global wiki_id
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    args, _ = get_args()
    wiki_id = args.wiki_id
    services = args.services.split(',')

    caching_dict = dict([(service+'.get', {'write_only': True}) for service in
                         services])
    use_caching(per_service_cache=caching_dict)

    log('Calling wiki-level services on %s' % args.wiki_id)

    #pool = Pool(processes=8)
    #s = pool.map_async(get_service, services)
    #s.wait()
    for service in services:
        get_service(service)


if __name__ == '__main__':
    main()
