import time
import warnings
import os
import argparse
import sys
warnings.filterwarnings(u'ignore', category=DeprecationWarning)
import gensim
import traceback
from boto import connect_s3
from collections import OrderedDict
from datetime import datetime
from . import launch_lda_nodes, terminate_lda_nodes, harakiri, ami
from . import log, get_dct_and_bow_from_features, write_csv_and_text_data


def get_args():
    ap = argparse.ArgumentParser(description=u"Perform latent dirichlet allocation against arbitrary CSV data")
    ap.add_argument(u'--num-topics', dest=u'num_topics', type=int,
                    default=os.getenv(u'NUM_TOPICS', 999),
                    help=u"Number of topics you want from the LDA process")
    ap.add_argument(u'--max-topic-frequency', dest=u'max_topic_frequency', type=int,
                    default=os.getenv(u'MAX_TOPIC_FREQUENCY', default=20000),
                    help=u"Threshold for number of intances a given topic appears in")
    ap.add_argument(u'--num-processes', dest=u"num_processes", type=int,
                    default=os.getenv(u'NUM_PROCESSES', 8),
                    help=u"Number of processes for async data access from S3")
    ap.add_argument(u'--model-prefix', dest=u'model_prefix', type=str,
                    default=os.getenv(u'MODEL_PREFIX', datetime.strftime(datetime.now(), u'%Y-%m-%d-%H-%M')),
                    help=u"Prefix to uniqueify model")
    ap.add_argument(u'--path-prefix', dest=u'path_prefix', type=str,
                    default=os.getenv(u'PATH_PREFIX', u"/mnt/"),
                    help=u"Prefix to path")
    ap.add_argument(u'--s3-prefix', dest=u's3_prefix', type=str,
                    default=os.getenv(u'S3_PREFIX', u"models/csv/"),
                    help=u"Prefix on s3 for model location")
    ap.add_argument(u'--auto-launch', dest=u'auto_launch', type=bool,
                    default=os.getenv(u'AUTOLAUNCH_NODES', True),
                    help=u"Whether to automatically launch distributed nodes")
    ap.add_argument(u'--instance-count', dest=u'instance_count', type=int,
                    default=os.getenv(u'NODE_INSTANCES', 20),
                    help=u"Number of node instances to launch")
    ap.add_argument(u'--node-ami', dest=u'ami', type=str,
                    default=os.getenv(u'NODE_AMI', ami),
                    help=u"AMI of the node machines")
    ap.add_argument(u'--dont-terminate-on-complete', dest=u'terminate_on_complete', action=u'store_false',
                    default=os.getenv(u'TERMINATE_ON_COMPLETE', True),
                    help=u"Prevent terminating this instance")
    ap.add_argument(u'--git-ref', dest=u'git_ref',
                    default=os.getenv(u'GIT_REF', u'master'),
                    help=u"A DSTK repo ref (tag, branch, commit hash) to check out")
    ap.add_argument(u'--s3file', dest=u's3file', default=os.getenv(U'S3FILE', None),
                    help=u'The location of the data file on S3')
    return ap.parse_args()


def get_feature_data(args):
    bucket = connect_s3().get_bucket(u'nlp-data')
    lines = bucket.get_key(args.s3file).get_contents_as_string().decode(u'utf8').split(u"\n")
    id_to_features = OrderedDict()
    for line in lines:
        splt = line.split(u',')
        id_to_features[splt[0]] = splt[1:]
    return id_to_features


def get_model_from_args(args):
    log(u"\n---LDA Model---")
    modelname = (u'%s-%s-lda-%s-csv-%stopics.model'
                 % (args.git_ref, args.model_prefix, args.s3file.replace(u'/', u'_'), args.num_topics))
    bucket = connect_s3().get_bucket(u'nlp-data')
    if os.path.exists(args.path_prefix+modelname):
        log(u"(loading from file)")
        lda_model = gensim.models.LdaModel.load(args.path_prefix+modelname)
    else:
        log(args.path_prefix+modelname, u"does not exist")
        key = bucket.get_key(args.s3_prefix+modelname)
        if key is not None:
            log(u"(loading from s3)")
            with open(u'/tmp/%s' % modelname, u'w') as fl:
                key.get_contents_to_file(fl)
            lda_model = gensim.models.LdaModel.load(u'/tmp/%s' % modelname)
        else:
            log(u"(building... this will take a while)")
            try:
                if args.auto_launch:
                    launching = launch_lda_nodes(args.instance_count, args.ami)
                log(u"Getting Data...")
                id_to_features = get_feature_data(args)
                log(u"Turning Data into Vectors")
                dct, bow_docs = get_dct_and_bow_from_features(id_to_features)
                log(u"Waiting for workers to get sorted out")
                launching.wait()
                log(u"Waiting an extra five minutes for workers to get their shit together")
                time.sleep(300)
                log(u"Finally building model from features")
                lda_model = gensim.models.LdaModel(bow_docs.values(),
                                                   num_topics=args.num_topics,
                                                   id2word=dict([(x[1], x[0]) for x in dct.token2id.items()]),
                                                   distributed=True)
                log(u"Done, saving model.")
                lda_model.save(args.path_prefix+modelname)
                log(u"uploading model to s3")
                key = bucket.new_key(u'%s%s/%s/%s' % (args.s3_prefix, args.git_ref, args.model_prefix, modelname))
                key.set_contents_from_file(open(args.path_prefix+modelname, u'r'))
                write_csv_and_text_data(args, bucket, modelname, id_to_features, bow_docs, lda_model)
                terminate_lda_nodes()
            except Exception as e:
                log(str(e))
                log(str(traceback.format_exc()))
                terminate_lda_nodes()
                return harakiri()
    return lda_model


def main():
    sys.stdout = os.fdopen(sys.stdout.fileno(), u'w', 0)
    args = get_args()
    get_model_from_args(args)
    log(u"Done")
    if args.terminate_on_complete:
        harakiri()


if __name__ == u'__main__':
    main()
