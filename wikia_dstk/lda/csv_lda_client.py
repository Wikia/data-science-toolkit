import argparse
import os
from boto import connect_s3
from datetime import datetime
from . import run_server_from_args, ami


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(u'--ami', dest=u'ami', type=str,
                        default=ami,
                        help=u'The AMI to launch')
    parser.add_argument(u'--num-nodes', dest=u'node_count', type=int,
                        default=20,
                        help=u"Number of worker nodes to launch")
    parser.add_argument(u'--num-topics', dest=u'num_topics', type=int, action=u'store',
                        default=os.getenv(u'NUM_TOPICS', 999),
                        help=u"The number of topics for the model to use")
    parser.add_argument(u'--path-prefix', dest=u'path_prefix', type=str, action=u'store',
                        default=os.getenv(u'PATH_PREFIX', u'/mnt/'),
                        help=u"Where to save the model")
    parser.add_argument(u'--max-topic-frequency', dest=u'max_topic_frequency', type=int,
                        default=os.getenv(u'MAX_TOPIC_FREQUENCY', 20000),
                        help=u"Threshold for number of instances a given topic appears in")
    parser.add_argument(u'--model-prefix', dest=u'model_prefix', type=str,
                        default=os.getenv(u'MODEL_PREFIX', datetime.strftime(datetime.now(), u'%Y-%m-%d-%H-%M')),
                        help=u"Prefix to uniqueify model")
    parser.add_argument(u'--s3-prefix', dest=u's3_prefix', type=str,
                        default=os.getenv(u'S3_PREFIX', u"models/csv/"),
                        help=u"Prefix on s3 for model location")
    parser.add_argument(u'--auto-launch', dest=u'auto_launch', type=bool,
                        default=os.getenv(u'AUTOLAUNCH_NODES', True),
                        help=u"Whether to automatically launch distributed nodes")
    parser.add_argument(u'--instance-count', dest=u'instance_count', type=int,
                        default=os.getenv(u'NODE_INSTANCES', 20),
                        help=u"Number of node instances to launch")
    parser.add_argument(u'--node-ami', dest=u'node_ami', type=str,
                        default=os.getenv(u'NODE_AMI', ami),
                        help=u"AMI of the node machines")
    parser.add_argument(u'--dont-terminate-on-complete', dest=u'terminate_on_complete', action=u'store_false',
                        default=os.getenv(u'TERMINATE_ON_COMPLETE', True),
                        help=u"Prevent terminating this instance")
    parser.add_argument(u'--master-ip', dest=u'master_ip', default=u'54.200.131.148',
                        help=u"The elastic IP address to associate with the master server")
    parser.add_argument(u'--killable', dest=u'killable', action=u'store_true', default=False,
                        help=u"Keyboard interrupt terminates master")
    parser.add_argument(u'--git-ref', dest=u'git_ref', default=u'master',
                        help=u"The git ref to use when deploying")
    parser.add_argument(u'--s3file', dest=u's3file', default=None,
                        help=u'The location of the data file on S3')
    parser.add_argument(u'--infile', dest=u'infile', default=None,
                        help=u'The filename you want to upload to s3 first.')
    return parser.parse_args()


def main():
    args = get_args()
    if not args.s3file:
        b = connect_s3().get_bucket(u'nlp-data')
        keyname = u'lda_csvs/'+args.infile
        k = b.new_key(keyname)
        k.set_contents_from_filename(args.infile)
        args.s3file = keyname

    run_server_from_args(args, u'wikia_dstk.lda.csv_lda_server',
                         user_data_extras=u"export S3FILE=%s" % args.s3file)


if __name__ == u'__main__':
    main()