import random
import time
from ..lda import harakiri
from . import get_argparser
from boto import connect_s3
from math import floor
from ..loadbalancing import EC2Connection


def log(string):
    # todo: real logging
    print string


def get_args():
    ap = get_argparser()
    return ap.parse_known_args()


def authority_user_data(args, s3_batch):
    ow = '--overwrite' if args.overwrite else ''
    return """#!/bin/bash
echo `date` `hostname -i ` "User Data Start" >> /var/log/my_startup.log
cd /home/ubuntu/WikiaAuthority
echo `date` `hostname -i ` "Updating WikiaAuthority" >> /var/log/my_startup.log
git fetch origin
git checkout %s
git pull origin %s
touch /var/log/authority
python -u etl_scaled.py --emit-events --s3file=%s %s --die-on-complete > /var/log/authority 2>&1 &
echo `date` `hostname -i ` "User Data End" >> /var/log/my_startup.log
""" % (args.authority_git_ref, args.authority_git_ref, s3_batch, ow)


def dstk_user_data(args):
    services = ['WikiAuthorsToIdsService', 'WikiAuthorsToPagesService', 'WikiTopicAuthorityService',
                'WikiAuthorTopicAuthorityService', 'WikiTopicsToAuthorityService']
    argstring = "--queue=authority_extraction_events --services=%s" % ",".join(services)
    return """#!/bin/bash
echo `date` `hostname -i ` "User Data Start" >> /var/log/my_startup.log
cd /home/ubuntu/data-science-toolkit
echo `date` `hostname -i ` "Updating DSTK" >> /var/log/my_startup.log
git fetch origin
git checkout %s
git pull origin %s && python setup.py install
touch /var/log/extraction
python -u -m wikia_dstk.pipeline.data_extraction.run %s > /var/log/extraction 2>&1 &
echo `date` `hostname -i ` "User Data End" >> /var/log/my_startup.log
""" % (args.dstk_git_ref, args.dstk_git_ref, argstring)


def main():
    args, _ = get_args()

    bucket = connect_s3().get_bucket('nlp-data')
    key = bucket.get_key(args.s3path)
    lines = key.get_contents_as_string().split("\n")
    authority_slice_size = int(floor(float(len(lines))/args.num_authority_nodes))
    authority_keys = []
    for i in range(0, len(lines), authority_slice_size):
        key = bucket.new_key('authority_events/%d' % random.randint(0, 100000000))
        key.set_contents_from_string("\n".join(lines[i:i+authority_slice_size]))
        authority_keys.append(key.name)

    log("Spinning up %d authority instances" % len(authority_keys))
    authority_params = dict(price='0.8', ami=args.authority_ami, tag="Authority Worker")
    authority_connection = EC2Connection(authority_params)
    user_data_scripts = map(lambda x: authority_user_data(args, x), authority_keys)
    print user_data_scripts
    r = authority_connection.add_instances_async(user_data_scripts, wait=False)
    authority_instance_ids = [i for j in r.get() for i in j]
    authority_connection.tag_instances(authority_instance_ids)
    log("Instance IDs are %s" % ','.join(authority_instance_ids))

    dstk_params = dict(price='0.8', ami=args.dstk_ami, tag="Authority Data Extraction")
    dstk_connection = EC2Connection(dstk_params)
    bucket = connect_s3().get_bucket('nlp-data')

    while True:
        num_authority_instances = len(authority_connection.get_tagged_instances())
        log("%d authority instances running..." % num_authority_instances)
        event_keys = bucket.get_all_keys(prefix='authority_extraction_events/')
        if len(event_keys) > 0:
            dstk_tagged = dstk_connection.get_tagged_instances()
            dstk_nodes_needed = args.num_data_extraction_nodes - len(dstk_tagged)
            if dstk_nodes_needed > 0:
                log("Spinning up %d DSTK nodes for data extraction" % dstk_nodes_needed)
                dstk_connection.add_instances(dstk_nodes_needed, dstk_user_data(args))
        elif num_authority_instances == 0:
            log("Empty queue and no authority instances, shutting down.")
            break

        time.sleep(120)

if __name__ == '__main__':
    main()