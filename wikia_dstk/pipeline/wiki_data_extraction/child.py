import os
import sys
from multiprocessing import Pool

from nlp_services.caching import use_caching
from config import config
from ... import get_argparser_from_config

# we dump everything in here to be dynamic
from nlp_services.discourse.entities import *
from nlp_services.discourse.sentiment import *
from nlp_services.syntax import *
from nlp_services.title_confirmation import *
from nlp_services.authority import *


def get_args():
    ap = get_argparser_from_config(config)
    ap.add_argument('-w', '--wiki-id', dest='wiki_id', required=True,
                    help="The wiki ID to operate over")
    return ap.parse_known_args()


def main():
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    args, _ = get_args()
    services = args.services.split(',')

    caching_dict = dict([(service+'.get', {'write_only': True}) for service in
                         services])
    use_caching(per_service_cache=caching_dict)

    print 'Calling wiki-level services on %s' % args.wiki_id

    def get_service(service):
        print args.wiki_id, service
        try:
            getattr(sys.modules[__name__], service)().get(args.wiki_id)
        except Exception as e:
            print e

    pool = Pool(processes=8)
    s = pool.map_async(services, get_service)
    s.wait()


if __name__ == '__main__':
    caching_dict[service+'.get'] = {'dont_compute': True}  # DRY!
    use_caching(per_service_cache=caching_dict)
    main()
