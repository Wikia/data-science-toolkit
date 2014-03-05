from __future__ import division
from .. import EC2Connection
from boto import connect_s3
from datetime import datetime
from math import ceil
from optparse import OptionParser, OptionError
from time import sleep

# Monitors the workload in specific intervals and scales up or down
# We need this script for two reasons:
# 1) You can't create metric alarms based off of stuff in S3
# 2) You can't create metric alarms for EC2 instances hosted outside of
# us-east-1

op = OptionParser()
op.add_option('-p', '--parser', dest='parser', action='store_true',
              default=False, help='Monitor parser instances')
op.add_option('-d', '--data-extraction', dest='data_ex', action='store_true',
              default=False, help='Monitor data extraction instances')
op.add_option('-r', '--region', dest='region',
              help='The EC2 region to connect to')
op.add_option('-c', '--cost', dest='price', help='The maximum bid price')
op.add_option('-a', '--ami', dest='ami', help='The AMI to use')
op.add_option('-k', '--key', dest='key', help='The name of the key pair')
op.add_option('-s', '--security-groups', dest='sec',
              help='The security groups with which to associate instances')
op.add_option('-i', '--instance-type', dest='type',
              help='The type of instance to run')
op.add_option('-t', '--tag', dest='tag',
              help='The name of the tag to operate over')
op.add_option('-e', '--threshold', dest='threshold', type='int',
              help='Acceptable number of events per process we will ' +
                   'tolerate as backlog')
op.add_option('-m', '--max-size', dest='max_size', type='int',
              help='The maximum allowable number of simultaneous instances')
(options, args) = op.parse_args()

if (options.parser and options.data_ex) or (not options.parser and
                                            not options.data_ex):
    raise OptionError('Specify one and only one process type to monitor',
                      'parser or data-extraction')
elif options.parser:
    from parser.config import config
elif options.data_ex:
    from data_extraction.config import config

config.update([(k, v) for (k, v) in vars(options).items() if v is not None])


s3_conn = connect_s3()
bucket = s3_conn.get_bucket('nlp-data')
ec2_conn = EC2Connection(config)

lastInQueue = None
intervals = []
while True:
    # Because it lists itself
    inqueue = len([k for k in bucket.list(config['queue'])]) - 1
    # Sometimes the directory gets deleted when empty
    if inqueue < 0:
        inqueue = 0
    instances = ec2_conn.get_tagged_instances(config['tag'])
    numinstances = len(instances)

    if not inqueue:
        print "[%s %s] Just chillin' (%d in queue, %d instances)" % (
            config['tag'], datetime.today().isoformat(' '), inqueue,
            numinstances)
        sleep(60)
        continue

    if not numinstances:
        optimal = int(ceil(inqueue / config['threshold']))
        if optimal <= config['max_size']:
            instances_to_add = optimal
        else:
            instances_to_add = config['max_size']
        ec2_conn.add_instances(instances_to_add)
        instances = ec2_conn.get_tagged_instances(config['tag'])
        numinstances = len(instances)
        print "[%s %s] Scaled up to %d (%d in queue)" % (
            config['tag'], datetime.today().isoformat(' '), numinstances,
            inqueue)
        continue

    if lastInQueue is not None and lastInQueue != inqueue:
        delta = (lastInQueue - inqueue)
        intervals.append((mins, delta * 250))
        avg = reduce(lambda x, y: x + y,
                     map(lambda x: x[1]/(x[0]*60), intervals))/len(intervals)
        rate = ", %.3f docs/sec; %d in the last %d minute(s)" % (avg,
                                                                 delta * 250,
                                                                 mins)
    else:
        rate = ""

    events_per_instance = inqueue / numinstances
    above_threshold = events_per_instance > config['threshold']

    if (config['max_size'] > numinstances and above_threshold):
        ratio = inqueue / numinstances
        while (ratio > config['threshold'] and
               numinstances < config['max_size']):
            optimal = int(ceil(inqueue / config['threshold'])) - numinstances
            allowed = config['max_size'] - numinstances
            instances_to_add = optimal if optimal <= allowed else allowed
            ec2_conn.add_instances(instances_to_add)
            instances = ec2_conn.get_tagged_instances(config['tag'])
            numinstances = len(instances)
            ratio = inqueue / numinstances
        print "[%s %s] Scaled up to %d (%d in queue%s)" % (
            config['tag'], datetime.today().isoformat(' '), numinstances,
            inqueue, rate)
    else:
        print "[%s %s] Just chillin' (%d in queue, %d instances%s)" % (
            config['tag'], datetime.today().isoformat(' '), inqueue,
            numinstances, rate)

    if inqueue == lastInQueue:
        mins += 1
    else:
        mins = 1

    lastInQueue = inqueue
    sleep(60)
