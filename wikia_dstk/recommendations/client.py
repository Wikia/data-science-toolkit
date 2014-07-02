from argparse import ArgumentParser, FileType
from datetime import datetime
from ..loadbalancing import EC2Connection
import time


def get_args():
    ap = ArgumentParser("Performs pairwise recommendations for a given topics file in S3.")
    ap.add_argument('--s3file', dest='s3file')
    ap.add_argument('--metric', dest="metric", default="cosine")
    ap.add_argument('--slice-size', dest='slice_size', default=100, type=int)
    ap.add_argument('--num-instances', dest='num_instances', type=int, default=10)
    ap.add_argument('--instance-batch-size', dest='instance_batch_size', type=int, default=20000)
    ap.add_argument('--recommendation-name', dest='recommendation_name', default='video')
    ap.add_argument('--num-topics', dest='num_topics', default=999, type=int)
    ap.add_argument('--git-ref', dest='git_ref', default='master')
    return ap.parse_args()


def get_user_data(args, datestamp):
    data = """#!/bin/bash
echo `date` `hostname -i ` "User Data Start" >> /var/log/my_startup.log
mkdir -p /mnt/
cd /home/ubuntu/data-science-toolkit
echo `date` `hostname -i ` "Updating DSTK" >> /var/log/my_startup.log
git fetch origin
git checkout %s
git pull origin %s && sudo python setup.py install
touch /var/log/recommender
python -u -m wikia_dstk.recommendations.server %s > /var/log/recommender 2>&1 &
echo `date` `hostname -i ` "User Data End" >> /var/log/my_startup.log
"""
    for i in range(0, args.num_instances):
        argstring = " ".join(["--s3file=%s" % args.s3file,
                              "--metric=%s" % args.metric,
                              "--slice-size=%d" % args.slice_size,
                              "--use-batches",
                              "--instance-batch-size=%d" % args.instance_batch_size,
                              "--instance-batch-offset=%d" % i,
                              "--recommendation-name=%s-%s" % (args.recommendation_name, datestamp),
                              "--num-topics=%d" % args.num_topics])
        yield data % (args.git_ref, args.git_ref, argstring)


def main():
    args = get_args()
    options = dict(price='0.8', ami='ami-4cdcb27c',
                   tag='recommender-%s' % args.recommendation_name)
    conn = EC2Connection(options)
    datestamp = str(datetime.strftime(datetime.now(), '%Y-%m-%d-%H-%M'))

    r = conn.add_instances_async(get_user_data(args, datestamp))
    start = time.time()
    while True:
        time.sleep(30)
        if r.ready():
            print "Ready after", time.time() - start, "seconds"
            break
        print "Been waiting for", time.time() - start, "seconds"
    result = r.get()
    print result
    instance_ids = [r for li in result for r in li]
    conn.tag_instances(instance_ids)
    print instance_ids


if __name__ == '__main__':
    main()
