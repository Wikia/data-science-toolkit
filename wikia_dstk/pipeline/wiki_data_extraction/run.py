import os
import random
import sys
from ..config.wiki_data_extraction import config
from boto import connect_s3
from boto.ec2 import connect_to_region
from boto.s3.prefix import Prefix
from boto.utils import get_instance_metadata
from subprocess import Popen, STDOUT
from time import sleep

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
BUCKET = connect_s3().get_bucket('nlp-data')

wids = [wid.strip() for wid in sys.argv[1].split(',')]
print "Working on %d wids" % len(wids)

processes = []
while len(wids) > 0:
    while len(processes) < 8:
        processes.append(Popen('/home/ubuntu/venv/bin/python -m wikia_dstk.pipeline.wiki_data_extraction.child %s' % wids.pop(), shell=True))

    processes = filter(lambda x: x.poll() is None, processes)
    sleep(0.25)

print "Scaling down, shutting down."
current_id = get_instance_metadata()['instance-id']
ec2_conn = connect_to_region(config['region'])
ec2_conn.terminate_instances([current_id])
