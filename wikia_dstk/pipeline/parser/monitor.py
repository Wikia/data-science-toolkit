from __future__ import division
from boto import connect_s3
from datetime import datetime
from math import ceil
from time import sleep
from ... import get_argparser_from_config
from ...loadbalancing import EC2Connection
from config import default_config

# Monitors the workload in specific intervals and scales up or down
# We need this script for two reasons:
# 1) You can't create metric alarms based off of stuff in S3
# 2) You can't create metric alarms for EC2 instances hosted outside of
# us-east-1

ap = get_argparser_from_config(default_config)
args, _ = ap.parse_known_args()

# Specific to parser
user_data = """
#!/bin/sh
cd /home/ubuntu/data-science-toolkit
git pull --rebase origin master
sudo python setup.py install
sudo sv restart parser_poller
"""

s3_conn = connect_s3()
bucket = s3_conn.get_bucket('nlp-data')
ec2_conn = EC2Connection(vars(args))

mins = 0
lastInQueue = None
intervals = []
while True:
    # Because it lists itself
    inqueue = len([k for k in bucket.list('text_events/')] + [j for j in bucket.list('text_bulk/')])
    # Sometimes the directory gets deleted when empty
    if inqueue < 0:
        inqueue = 0
    instances = ec2_conn.get_tagged_instances(args.tag)
    numinstances = len(instances)

    # Make sure tagged instances are still running, reboot if not
    #ec2_conn.ensure_instance_health(args.tag)

    if not inqueue:
        print "[%s %s] Just chillin' (%d in queue, %d instances)" % (
            args.tag, datetime.today().isoformat(' '), inqueue,
            numinstances)
        sleep(60)
        continue

    if not numinstances:
        optimal = int(ceil(inqueue / args.threshold))
        if optimal <= args.max_size:
            instances_to_add = optimal
        else:
            instances_to_add = args.max_size
        ec2_conn.add_instances(instances_to_add, user_data=user_data, instance_type="parser")
        instances = ec2_conn.get_tagged_instances(args.tag)
        numinstances = len(instances)
        print "[%s %s] Scaled up to %d (%d in queue)" % (
            args.tag, datetime.today().isoformat(' '), numinstances,
            inqueue)
        continue

    if lastInQueue is not None and lastInQueue != inqueue:
        delta = (lastInQueue - inqueue)
        intervals.append((mins, delta))
        avg = reduce(lambda x, y: x + y,
                     map(lambda x: x[1]/(x[0]), intervals))/len(intervals)
        rate = ", %.3f tarballs/min; %d in the last %d minute(s)" % (avg, mins)
    else:
        rate = ""

    events_per_instance = inqueue / numinstances
    above_threshold = events_per_instance > args.threshold

    if (args.max_size > numinstances and above_threshold):
        ratio = inqueue / numinstances
        while (ratio > args.threshold and
               numinstances < args.max_size):
            optimal = int(ceil(inqueue / args.threshold)) - numinstances
            allowed = args.max_size - numinstances
            instances_to_add = optimal if optimal <= allowed else allowed
            ec2_conn.add_instances(instances_to_add, user_data=user_data)
            instances = ec2_conn.get_tagged_instances(args.tag)
            numinstances = len(instances)
            ratio = inqueue / numinstances
        print "[%s %s] Scaled up to %d (%d in queue%s)" % (
            args.tag, datetime.today().isoformat(' '), numinstances,
            inqueue, rate)
    else:
        print "[%s %s] Just chillin' (%d in queue, %d instances%s)" % (
            args.tag, datetime.today().isoformat(' '), inqueue,
            numinstances, rate)

    if inqueue == lastInQueue:
        mins += 1
    else:
        mins = 1

    lastInQueue = inqueue
    sleep(60)
