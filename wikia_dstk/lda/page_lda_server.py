import argparse
import gensim
import json
import os
import sys
import time
import traceback
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
from nlp_services.caching import use_caching
from nlp_services.syntax import WikiToPageHeadsService
from nlp_services.title_confirmation import preprocess
from nlp_services.discourse.entities import WikiPageToEntitiesService
from boto import connect_s3
from boto.exception import EC2ResponseError
from datetime import datetime
from . import normalize, launch_lda_nodes, terminate_lda_nodes, harakiri
from . import get_dct_and_bow_from_features, write_csv_and_text_data
from .. import log

bucket = connect_s3().get_bucket('nlp-data')


def get_args():
    ap = argparse.ArgumentParser(
        description="Generate a per-page topic model using latent dirichlet " +
        "analysis.")
    ap.add_argument('--overwrite-existing-model', dest='overwrite',
                    action='store_true', default=False,
                    help="Overwrite existing LDA model on S3")
    ap.add_argument('--wiki-id', dest='wiki_id', type=str,
                    default=os.getenv('WIKI_ID'),
                    help="The source file of wiki IDs sorted by WAM")
    ap.add_argument('--num-topics', dest='num_topics', type=int,
                    default=os.getenv('NUM_TOPICS', 999),
                    help="Number of topics you want from the LDA process")
    ap.add_argument('--max-topic-frequency', dest='max_topic_frequency',
                    type=int, default=os.getenv('MAX_TOPIC_FREQUENCY', 500),
                    help="Threshold for number of wikis a given topic appears in")
    ap.add_argument('--num-processes', dest="num_processes", type=int,
                    default=os.getenv('NUM_PROCESSES', 8),
                    help="Number of processes for async data access from S3")
    ap.add_argument('--model-prefix', dest='model_prefix', type=str,
                    default=os.getenv(
                        'MODEL_PREFIX', datetime.strftime(
                            datetime.now(), '%Y-%m-%d-%H-%M')),
                    help="Prefix to uniqueify model")
    ap.add_argument('--path-prefix', dest='path_prefix', type=str,
                    default=os.getenv('PATH_PREFIX', "/mnt/"),
                    help="Prefix to path")
    ap.add_argument('--s3-prefix', dest='s3_prefix', type=str,
                    default=os.getenv('S3_PREFIX', "models/page/"),
                    help="Prefix on s3 for model location")
    ap.add_argument('--auto-launch', dest='auto_launch', type=bool,
                    default=os.getenv('AUTOLAUNCH_NODES', True),
                    help="Whether to automatically launch distributed nodes")
    ap.add_argument('--instance-count', dest='instance_count', type=int,
                    default=os.getenv('NODE_INSTANCES', 20),
                    help="Number of node instances to launch")
    ap.add_argument('--node-ami', dest='ami', type=str,
                    default=os.getenv('NODE_AMI', "ami-40701570"),
                    help="AMI of the node machines")
    ap.add_argument('--dont-terminate-on-complete',
                    dest='terminate_on_complete', action='store_false',
                    default=os.getenv('TERMINATE_ON_COMPLETE', True),
                    help="Prevent terminating this instance")
    ap.add_argument('--git-ref', dest='git_ref',
                    default=os.getenv('GIT_REF', 'master'),
                    help="A DSTK repo ref (tag, branch, commit hash) to check out")
    return ap.parse_args()


def get_data(wid):
    log(wid)
    use_caching(shouldnt_compute=True)
    #should be CombinedEntitiesService yo
    doc_ids_to_heads = WikiToPageHeadsService().get_value(wid, {})
    doc_ids_to_entities = WikiPageToEntitiesService().get_value(wid, {})
    doc_ids_combined = {}
    if doc_ids_to_heads == {}:
        log(wid, "no heads")
    if doc_ids_to_entities == {}:
        log(wid, "no entities")
    from_s3 = json.loads(bucket.get_key(
        'feature-data/page-%s.json' % wid).get_contents_as_string())
    for doc_id in doc_ids_to_heads:
        entity_response = doc_ids_to_entities.get(
            doc_id, {'titles': [], 'redirects': {}})
        doc_ids_combined[doc_id] = (map(preprocess,
                                        entity_response['titles'] +
                                        entity_response['redirects'].keys() +
                                        entity_response['redirects'].values() +
                                        list(set(doc_ids_to_heads.get(doc_id,
                                                                      [])))) +
                                    from_s3.get(doc_id, []))
    return doc_ids_combined.items()


def get_feature_data(args):
    wid = args.wiki_id
    log("Working on", wid)
    doc_id_to_terms = {}
    for (pid, list_of_terms) in get_data(wid):
        normalized = []
        for term in list_of_terms:
            tokens = [normalize(token) for token in term.split(' ')]
            normalized.append('_'.join(tokens))
        doc_id_to_terms[pid] = normalized
    return doc_id_to_terms


def get_model_from_args(args):
    log("\n---LDA Model---")
    modelname = '%s-%s-page-lda-wid-%s-%stopics.model' % (
        args.git_ref, args.model_prefix, args.wiki_id, args.num_topics)
    if os.path.exists(args.path_prefix+modelname):
        log("(loading from file)")
        lda_model = gensim.models.LdaModel.load(args.path_prefix+modelname)
    else:
        log(args.path_prefix+modelname, "does not exist")
        key = bucket.get_key(args.s3_prefix+modelname)
        if not args.overwrite and key is not None:
            log("(loading from s3)")
            with open('/tmp/%s' % modelname, 'w') as fl:
                key.get_contents_to_file(fl)
            lda_model = gensim.models.LdaModel.load('/tmp/%s' % modelname)
        else:
            log("(building... this will take a while)")
            try:
                if args.auto_launch:
                    launching = launch_lda_nodes(args.instance_count, args.ami)
                log("Getting Data...")
                doc_id_to_terms = get_feature_data(args)
                log("Turning Data into Vectors")
                dct, bow_docs = get_dct_and_bow_from_features(doc_id_to_terms)
                log("Waiting for workers to get sorted out")
                launching.wait()
                log("Waiting an extra five minutes for workers to get their " +
                    "shit together")
                time.sleep(300)
                log("Finally building model from features")
                lda_model = gensim.models.LdaModel(
                    bow_docs.values(), num_topics=args.num_topics,
                    id2word=dict([(x[1], x[0]) for x in dct.token2id.items()]),
                    distributed=True)
                log("Done, saving model.")
                lda_model.save(args.path_prefix+modelname)
                write_csv_and_text_data(args, bucket, modelname,
                                        doc_id_to_terms, bow_docs, lda_model)
                log("uploading model to s3")
                key = bucket.new_key(
                    '%s%s/%s/%s' % (args.s3_prefix, args.git_ref,
                                    args.model_prefix, modelname))
                key.set_contents_from_file(
                    open(args.path_prefix+modelname, 'r'))
                terminate_lda_nodes()
            #except EC2ResponseError:
            #    terminate_lda_nodes()
            #    return harakiri()
            except Exception as e:
                log(e)
                log(traceback.format_exc())
                #terminate_lda_nodes()  # keep commented out
                return
                #return harakiri()  # keep commented out
    return lda_model


def main():
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    use_caching()
    args = get_args()
    get_model_from_args(args)
    log("Done")
    #if args.terminate_on_complete:
    #    harakiri()


if __name__ == '__main__':
    start = time.time()
    main()
    log(time.time() - start, 'seconds elapsed')
