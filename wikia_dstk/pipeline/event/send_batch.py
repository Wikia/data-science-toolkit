"""
Iterates over files in the text directory, attempts to tar them in batches of a
specified size, optionally uploads them to S3, and cleans up the original files.
"""

import logging
import os
import requests
import shutil
import sys
import tarfile
import traceback
from . import chrono_sort, ensure_dir_exists
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from optparse import OptionParser
from time import sleep
from uuid import uuid4

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

# Allow user to configure options
parser = OptionParser()
parser.add_option('-b', '--batchsize', dest='batchsize', type='int', action='store', default=500, help='Specify the maximum number of files in a .tgz batch')
(options, args) = parser.parse_args()

BATCHSIZE = options.batchsize
LOCAL = options.local

TEXT_DIR = ensure_dir_exists('/data/text/')
TEMP_TEXT_DIR = ensure_dir_exists('/data/temp_text/')

bucket = S3Connection().get_bucket('nlp-data')

if __name__ == '__main__':

    # Set to run indefinitely
    while True:

        try:
            bypass_minimum = False
            # Attempt to enforce minimum batch size, continue after 30 seconds if not
            logger.debug('Checking # of files in text directory...')
            num_text_files = len(os.listdir(TEXT_DIR))
            logger.info('There are %i files in the text directory.' % num_text_files)
            if num_text_files == 0:
                logger.info('Waiting 60 seconds for text directory to populate...')
                sleep(60)
                continue
            if num_text_files < BATCHSIZE:
                logger.warning('Current batch does not meet %i file minimum, waiting for 60 seconds...' % BATCHSIZE)
                bypass_minimum = True
                sleep(60)
            logger.info('Sorting text files chronologically.')
            text_files = chrono_sort(TEXT_DIR)

            for n in range(0, len(text_files), BATCHSIZE):
                files_left = len(text_files) - n
                if files_left < BATCHSIZE:
                    if not bypass_minimum:
                        logger.warning('Exhausted chronological file list; refreshing.')
                        break
                # Move text files to temp directory
                text_batch_dir = ensure_dir_exists(os.path.join(TEMP_TEXT_DIR, str(uuid4())))
                for text_file in text_files[n:n+BATCHSIZE]:
                    shutil.move(text_file[0], os.path.join(text_batch_dir, os.path.basename(text_file[0])))
                logger.info('Moving batch to %s; %i files left.' % (text_batch_dir, files_left))

                # Tar batch
                tarball_path = text_batch_dir + '.tgz'
                logger.info('Archiving batch to %s' % tarball_path)
                tarball = tarfile.open(tarball_path, 'w:gz')
                tarball.add(text_batch_dir, '.')
                tarball.close()

                # Remove temp directory
                shutil.rmtree(text_batch_dir)

                # Upload to S3
                logger.info('Uploading %s to S3' % os.path.basename(tarball_path))
                k = Key(bucket)
                k.key = 'text_events/%s' % os.path.basename(tarball_path)
                k.set_contents_from_filename(tarball_path)
                os.remove(tarball_path)

        except KeyboardInterrupt:
            sys.exit(0)
        except:
            logger.error(traceback.format_exc())
