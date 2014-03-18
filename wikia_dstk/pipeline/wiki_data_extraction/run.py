import argparse
import os
import sys
from boto import connect_s3
from boto.ec2 import connect_to_region
from boto.s3.key import Key
from boto.utils import get_instance_metadata
from subprocess import Popen
from time import sleep

from config import config

ap = argparse.ArgumentParser()
# TODO: Need backoff for --wikis if file doesn't exist
ap.add_argument('-w', '--wikis', dest='wikis', type=str,
                help='File containing wiki IDs to run wiki data extraction on')
ap.add_argument('-r', '--region', dest='region', type=str,
                default=config['region'], help='EC2 region to connect to')
args = ap.parse_args()

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

# Pull wiki IDs from file on S3
bucket = connect_s3().get_bucket('nlp-data')
k = Key(bucket)
k.key = args.wikis
wids = [wid.strip() for wid in k.get_contents_as_string().split(',')]
k.delete()
print "Working on %d wids" % len(wids)

processes = []
while len(wids) > 0:
    while len(processes) < 8:
        if wids:
            wid = wids.pop()
            print 'Launching child to process %s' % wid
            processes.append(
                Popen('/home/ubuntu/venv/bin/python -m ' +
                      'wikia_dstk.pipeline.wiki_data_extraction.child %s' % wid,
                      shell=True))
        else:
            print 'No more wiki IDs to iterate over'
            break

    processes = filter(lambda x: x.poll() is None, processes)
    sleep(0.25)

for i in range(10):
    n = i + 1
    print 'Waiting for 5 minutes and shutting down. 30sec interval %d/10' % n
    sleep(30)

print "Scaling down, shutting down."
current_id = get_instance_metadata()['instance-id']
ec2_conn = connect_to_region(args.region)
ec2_conn.terminate_instances([current_id])