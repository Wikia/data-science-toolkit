# TODO: use multiprocessing.Pool instead of Queue, since we only expect 4 (a
# static number of) event files

# Iterates over query queue files and writes text from queries specified in the
# query queue files.

import logging
import os
import shutil
import sys
import traceback
from . import QueryIterator, clean_list
from ... import ensure_dir_exists
from multiprocessing import Pool
from optparse import OptionParser
from time import sleep

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

# Allow user to configure options
parser = OptionParser()
parser.add_option('-n', '--workers', dest='workers', type='int',
                  action='store', default=4,
                  help='Specify the number of worker processes to open')
(options, args) = parser.parse_args()

EVENT_DIR = ensure_dir_exists('/data/events/')
TEMP_EVENT_DIR = ensure_dir_exists('/data/temp_events/')
TEXT_DIR = ensure_dir_exists('/data/text/')
TEMP_TEXT_DIR = ensure_dir_exists('/data/temp_text/')


def write_text(event_file):
    """
    Write text from Solr queries in an event file to TEXT_DIR

    :type event_file: string
    :param event_file: Path to the event file containing Solr queries

    :rtype: string
    :return: A string indicating the name of the completed event file
    """
    try:
        temp_event_file = os.path.join(
            TEMP_EVENT_DIR, os.path.basename(event_file))
        shutil.move(event_file, temp_event_file)
        for line in open(temp_event_file):
            query = line.strip()
            logger.info('Writing query: "%s"' % query)
            qi = QueryIterator(
                'http://search-9.prod.wikia.net:8983/solr/main/',
                {'query': query, 'fields': 'id,wid,html_en,indexed',
                 'sort': 'id asc'})
            for doc in qi:
                if doc['id'].count('_') > 1:
                    # i love adding logic to my scripts just to avoid garbage, thanks guys
                    continue
                # Sanitize and write text
                text = '\n'.join(clean_list(doc.get('html_en', '')))
                localpath = os.path.join(TEXT_DIR, doc['id'])
                logger.debug('Writing text from %s to %s' % (doc['id'],
                                                             localpath))
                with open(localpath, 'w') as f:
                    f.write(text)
        os.remove(temp_event_file)
        return 'Finished event file %s' % event_file
    except KeyboardInterrupt:
        sys.exit(0)
    except:
        return '%s: %s' % (event_file, traceback.format_exc())


if __name__ == '__main__':
    while True:
        # List of query queue files to iterate over
        event_files = [os.path.join(EVENT_DIR, event_file) for event_file in
                       os.listdir(EVENT_DIR)]
        logger.info('Iterating over %i event files...' % len(event_files))

        # If there are no query queue files present, wait and retry
        if not event_files:
            logger.info('No event files found, waiting for 60 seconds...')
            sleep(60)
            continue

        for status in Pool(processes=options.workers).map(write_text,
                                                          event_files):
            print status
