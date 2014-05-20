import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
import argparse
import os
import gensim
import time
import json
from . import launch_lda_nodes, terminate_lda_nodes, log, harakiri, ami
from . import video_json_key, get_dct_and_bow_from_features, write_csv_and_text_data
from boto import connect_s3


def get_args():
    parser = argparse.ArgumentParser(description='Generate a per-page topic model using latent dirichlet analysis.')
    parser.add_argument('--num-topics', dest='num_topics', type=int, action='store',
                        default=os.getenv('NUM_TOPICS', 999),
                        help="The number of topics for the model to use")
    parser.add_argument('--path-prefix', dest='path_prefix', type=str, action='store',
                        default=os.getenv('PATH_PREFIX', '/mnt/'),
                        help="Where to save the model")
    parser.add_argument('--max-topic-frequency', dest='max_topic_frequency', type=int,
                        default=os.getenv('MAX_TOPIC_FREQUENCY', 50000),
                        help="Threshold for number of videos a given topic appears in")
    parser.add_argument('--model-prefix', dest='model_prefix', type=str,
                        default=os.getenv('MODEL_PREFIX', time.time()),
                        help="Prefix to uniqueify model")
    parser.add_argument('--s3-prefix', dest='s3_prefix', type=str,
                        default=os.getenv('S3_PREFIX', "models/video/"),
                        help="Prefix on s3 for model location")
    parser.add_argument('--auto-launch', dest='auto_launch', type=bool,
                        default=os.getenv('AUTOLAUNCH_NODES', True),
                        help="Whether to automatically launch distributed nodes")
    parser.add_argument('--instance-count', dest='instance_count', type=int,
                        default=os.getenv('NODE_INSTANCES', 20),
                        help="Number of node instances to launch")
    parser.add_argument('--node-ami', dest='node_ami', type=str,
                        default=os.getenv('NODE_AMI', ami),
                        help="AMI of the node machines")
    parser.add_argument('--dont-terminate-on-complete', dest='terminate_on_complete', action='store_false',
                        default=os.getenv('TERMINATE_ON_COMPLETE', True),
                        help="Prevent terminating this instance")
    parser.add_argument('--git-ref', dest='git_ref',
                        default=os.getenv('GIT_REF', 'master'),
                        help="A DSTK repo ref (tag, branch, commit hash) to check out")
    return parser.parse_args()


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
            async_result = launch_lda_nodes(instance_count=args.instance_count, ami=args.node_ami)
            log("Getting features while LDA nodes launch")
            doc_id_to_terms = json.loads(bucket.get_key(video_json_key).get_contents_as_string())
            dct, bow_docs = get_dct_and_bow_from_features(doc_id_to_terms)
            log("Got features, building model")
            log("Waiting for spot instances to load...")
            async_result.wait()
            log("Waiting an extra five minutes for shit to pop off")
            time.sleep(300)
            lda_model = gensim.models.LdaModel(bow_docs.values(),
                                               num_topics=args.num_topics,
                                               id2word=dict([(x[1], x[0]) for x in dct.token2id.items()]),
                                               distributed=True)
            log("Done, saving model.")
            lda_model.save(model_location)
            write_csv_and_text_data(args, bucket, modelname, doc_id_to_terms, bow_docs, lda_model)
            log("uploading model to s3")
            key = bucket.new_key(args.s3_prefix+modelname)
            key.set_contents_from_filename(model_location)
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
