import os
import sys
import traceback
from boto import connect_s3

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
    ap.add_argument('-w', '--wiki-id', dest='wiki_id', required=True, help="The wiki ID to operate over")
    return ap.parse_known_args()


def main():
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    args, _ = get_args()
    services = args.services.split(',')

    caching_dict = dict([(service+'.get', {'write_only': True}) for service in
                         services])
    use_caching(per_service_cache=caching_dict)

    print 'Calling wiki-level services on %s' % args.wiki_id
    try:
        for service in services:
            try:
                print args.wiki_id, service
                getattr(sys.modules[__name__], service)().get(args.wiki_id)
                caching_dict[service+'.get'] = {'dont_compute': True}  # DRY!
                use_caching(per_service_cache=caching_dict)
            except Exception as e:
                print args.wiki_id, service, e
    except Exception as e:
        print args.wiki_id, e


if __name__ == '__main__':
    main()