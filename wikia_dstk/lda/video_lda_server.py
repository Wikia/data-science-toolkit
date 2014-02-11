import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
import argparse
import os
import gensim
import time
import json
from multiprocessing import Pool
from . import launch_lda_nodes, terminate_lda_nodes, log, harakiri
from . import video_json_key, get_dct_and_bow_from_features, write_csv_and_text_data
from boto import connect_s3


def get_args():
    parser = argparse.ArgumentParser(description='Generate a per-page topic model using latent dirichlet analysis.')
    parser.add_argument('--data-file', dest='datafile', nargs='?', type=argparse.FileType('r'),
                        help="The source file of video features")
    parser.add_argument('--num-topics', dest='num_topics', type=int, action='store',
                        help="The number of topics for the model to use")
    parser.add_argument('--path-prefix', dest='path_prefix', type=str, action='store',
                        default=os.getenv('PATH_PREFIX', '/mnt/'),
                        help="Where to save the model")
    parser.add_argument('--max-topic-frequency', dest='max_topic_frequency', type=int,
                        default=os.getenv('MAX_TOPIC_FREQUENCY', 50000),
                        help="Threshold for number of videos a given topic appears in")
    parser.add_argument('--num-processes', dest="num_processes", type=int,
                        default=os.getenv('NUM_PROCESSES', 8),
                        help="Number of processes for async moves")
    parser.add_argument('--model-prefix', dest='model_prefix', type=str,
                        default=os.getenv('MODEL_PREFIX', time.time()),
                        help="Prefix to uniqueify model")
    parser.add_argument('--path-prefix', dest='path_prefix', type=str,
                        default=os.getenv('PATH_PREFIX', "/mnt/"),
                        help="Prefix to path")
    parser.add_argument('--s3-prefix', dest='s3_prefix', type=str,
                        default=os.getenv('S3_PREFIX', "models/wiki/"),
                        help="Prefix on s3 for model location")
    parser.add_argument('--auto-launch', dest='auto_launch', type=bool,
                        default=os.getenv('AUTOLAUNCH_NODES', True),
                        help="Whether to automatically launch distributed nodes")
    parser.add_argument('--instance-count', dest='instance_count', type=int,
                        default=os.getenv('NODE_INSTANCES', 20),
                        help="Number of node instances to launch")
    parser.add_argument('--node-ami', dest='node_ami', type=str,
                        default=os.getenv('NODE_AMI', "ami-40701570"),
                        help="AMI of the node machines")
    parser.add__argument('--dont-terminate-on-complete', dest='terminate_on_complete', action='store_false',
                         default=os.getenv('TERMINATE_ON_COMPLETE', True),
                         help="Prevent terminating this instance")
    return parser.parse_args()


def get_data(line):
    split = line.split('\t')
    split_filtered = filter(lambda y: y != '' and len(y.split('~')) > 1 and y.split('~')[1].strip() != '', split[1:])
    return [(split[0], split_filtered)]


def get_model_from_args(args):
    log("\n---LDA Model---")
    modelname = '%s-video-%dtopics.model' % (args.model_prefix, args.num_topics)
    model_location = args.path_prefix+'/'+modelname
    bucket = connect_s3().get_bucket('nlp-data')
    if os.path.exists(model_location):
        log("(loading from file)")
        lda_model = gensim.models.LdaModel.load(model_location)
    else:
        log(model_location, "does not exist")
        key = bucket.get_key(args.s3_prefix+modelname)
        if key is not None:
            log("(loading from s3)")
            with open('/tmp/%s' % modelname, 'w') as fl:
                key.get_contents_to_file(fl)
            lda_model = gensim.models.LdaModel.load('/tmp/%s' % modelname)
        else:
            log("(building...)")
            launch_lda_nodes()
            if args.datafile:
                pool = Pool(processes=args.num_processes)
                r = pool.map_async(get_data, args.datafile)
                doc_id_to_terms = dict(r.get())
            else:
                doc_id_to_terms = json.loads(bucket.get_key(video_json_key).get_contents_as_string())
            dct, bow_docs = get_dct_and_bow_from_features(doc_id_to_terms)
            lda_model = gensim.models.LdaModel(bow_docs.values(),
                                               num_topics=args.num_topics,
                                               id2word=dict([(x[1], x[0]) for x in dct.token2id.items()]),
                                               distributed=True)
            log("Done, saving model.")
            lda_model.save(model_location)
            write_csv_and_text_data(args, bucket, modelname, doc_id_to_terms, bow_docs, lda_model)
            log("uploading model to s3")
            key = bucket.new_key(args.s3_prefix+modelname)
            key.set_contents_from_file(args.path_prefix+modelname)
            terminate_lda_nodes()
    return lda_model


def main():
    args = get_args()
    get_model_from_args(args)
    log("Done")
    if args.terminate_on_complete:
        harakiri()


if __name__ == '__main__':
    main()