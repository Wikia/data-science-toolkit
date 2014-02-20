# TODO: use multiprocessing.Pool instead of Queue, since we only expect 4 (a static number of) event files

"""
Iterates over query queue files and writes text from queries specified in the
query queue files.
"""

import json
import logging
import os
import shutil
import traceback
from . import QueryIterator, clean_list, ensure_dir_exists
from time import sleep
from optparse import OptionParser
from multiprocessing import Process, Queue
from multiprocessing.process import current_process

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

# Allow user to configure options
parser = OptionParser()
parser.add_option('-n', '--workers', dest='workers', type='int', action='store', default=4, help='Specify the number of worker processes to open')
(options, args) = parser.parse_args()

EVENT_DIR = ensure_dir_exists('/data/events/')
TEMP_EVENT_DIR = ensure_dir_exists('/data/temp_events/')
TEXT_DIR = ensure_dir_exists('/data/text/')
TEMP_TEXT_DIR = ensure_dir_exists('/data/temp_text/')

def write_text(event_file):
    """Takes event file as input, writes text from all queries contained in
    event file to TEXT_DIR"""
    for line in open(event_file):
        query = line.strip()
        logger.info('Writing query from %s: "%s"' % (current_process(), query))
        qi = QueryIterator('http://search-s10.prod.wikia.net:8983/solr/main/', {'query': query, 'fields': 'id,wid,html_en,indexed', 'sort': 'id asc'})
        for doc in qi:
            # Sanitize and write text
            text = '\n'.join(clean_list(doc.get('html_en', '')))
            localpath = os.path.join(TEXT_DIR, doc['id'])
            logger.debug('Writing text from %s to %s' % (doc['id'], localpath))
            with open(localpath, 'w') as f:
                f.write(text)
    return 'Finished event file %s' % event_file

def write_worker(event_queue, result_queue):
    """Takes queue of event files, moves each file to TEMP_EVENT_DIR, calls
    write_text() on the aforementioned file, and adds the returned list of
    written files to a queue of text files"""
    for event_file in iter(event_queue.get, None):
        try:
            results = write_text(event_file)
            for result in results:
                result_queue.put(result)
            os.remove(event_file)
        except:
            logger.error(traceback.format_exc())

if __name__ == '__main__':
    event_queue = Queue()
    result_queue = Queue()

    while True:
        # List of query queue files to iterate over
        event_files = [os.path.join(EVENT_DIR, event_file) for event_file in os.listdir(EVENT_DIR)]
        logger.info('Iterating over %i event files...' % len(event_files))

        # If there are no query queue files present, wait and retry
        if not event_files:
            logger.info('No event files found, waiting for 60 seconds...')
            sleep(60)
            continue

        for event_file in event_files:
            temp_event_file = os.path.join(TEMP_EVENT_DIR, os.path.basename(event_file))
            shutil.move(event_file, temp_event_file)
            event_queue.put(temp_event_file)

        workers = [Process(target=write_worker, args=(event_queue, result_queue)) for n in range(options.workers)]

        for worker in workers:
            worker.start()
