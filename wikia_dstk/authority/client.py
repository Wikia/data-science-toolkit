import random
import time
from ..lda import harakiri
from logging import getLogger
from . import get_argparser
from boto import connect_s3
from math import floor
from ..loadbalancing import EC2Connection


def get_args():
    ap = get_argparser()
    return ap.parse_known_args()


def authority_user_data(args, s3_batch):
    return """#!/bin/bash
echo `date` `hostname -i ` "User Data Start" >> /var/log/my_startup.log
cd /home/ubuntu/WikiaAuthority
echo `date` `hostname -i ` "Updating WikiaAuthority" >> /var/log/my_startup.log
git fetch origin
git checkout %s
git pull origin %s
touch /var/log/authority
python -u etl_scaled.py --s3file=%s --overwrite=%s --die-on-complete > /var/log/authority 2>&1 &
echo `date` `hostname -i ` "User Data End" >> /var/log/my_startup.log
""" % (args.authority_git_ref, args.authority_git_ref, s3_batch, args.overwrite)


def dstk_user_data(args):
    services = ['WikiAuthorsToIdsService', 'WikiAuthorsToPagesService', 'WikiTopicAuthorityService',
                'WikiAuthorTopicAuthorityService', 'WikiTopicsToAuthorityService']
    return """#!/bin/bash
echo `date` `hostname -i ` "User Data Start" >> /var/log/my_startup.log
cd /home/ubuntu/data-science-toolkit
echo `date` `hostname -i ` "Updating DSTK" >> /var/log/my_startup.log
git fetch origin
git checkout %s
git pull origin %s && python setup.py install
touch /var/log/extraction
python -u -m wikia_dstk.pipeline.data_extraction.run --queue=authority_events --services=%s > /var/log/authority 2>&1 &
echo `date` `hostname -i ` "User Data End" >> /var/log/my_startup.log
""" % (args.dstk_git_ref, args.dstk_git_ref, ",".join(services))


def main():
    args, _ = get_args()
    logger = getLogger('wikia_dstk.authority')

    bucket = connect_s3().get_bucket('nlp-data')
    key = bucket.get_key(args.s3path)
    lines = key.get_contents_as_string().split("\n")
    authority_slice_size = floor(len(lines)/args.num_authority_nodes)
    authority_keys = []
    for i in range(0, len(lines), floor(authority_slice_size)):
        key = bucket.new_key()
        key.name = 'authority_events/'+random.randint(0, 100000000)
        key.set_contents_from_string("\n".join(lines[i:i+authority_slice_size]))
        authority_keys.append(key.name)

    logger.info("Spinning up", len(authority_keys), "authority instances")
    authority_params = dict(price='0.8', ami=args.authority_ami, tag="Authority Worker")
    authority_connection = EC2Connection(params=authority_params)
    r = authority_connection.add_instance_async(map(lambda x: authority_user_data(args, x), authority_keys), wait=False)
    authority_instance_ids = [i for j in r.get() for i in j]
    logger.info("Instance IDs are %d" % ','.join(authority_instance_ids))

    dstk_params = dict(price='0.8', ami=args.dstk_ami, tag="Authority Data Extraction")
    dstk_connection = EC2Connection(params=dstk_params)
    bucket = connect_s3().get_bucket('nlp-data')

    while True:
        num_authority_instances = len(authority_connection.get_tagged_instances())
        logger.info("%d authority instances running..." % num_authority_instances)
        event_keys = bucket.get_all_keys(prefix='authority_events/')
        if len(event_keys) > 0:
            dstk_tagged = dstk_connection.get_tagged_instances()
            dstk_nodes_needed = args.num_data_extraction_nodes - len(dstk_tagged)
            if dstk_nodes_needed > 0:
                logger.info("Spinning up %d DSTK nodes for data extraction" % dstk_nodes_needed)
                dstk_connection.add_instances(dstk_nodes_needed, dstk_user_data(args))
        elif num_authority_instances == 0:
            logger.info("Empty queue and no authority instances, shutting down.")
            harakiri()

        time.sleep(120)

if __name__ == '__main__':
    main()