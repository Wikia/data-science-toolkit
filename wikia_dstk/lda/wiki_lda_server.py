import time
import warnings
import os
import requests
import argparse
import sys
warnings.filterwarnings('ignore', category=DeprecationWarning)
import gensim
import traceback
from nlp_services.discourse.entities import TopEntitiesService
from nlp_services.syntax import HeadsCountService
from nlp_services.caching import use_caching
from multiprocessing import Pool
from boto import connect_s3
from collections import defaultdict
from datetime import datetime
from . import normalize, unis_bis, launch_lda_nodes, terminate_lda_nodes, harakiri
from . import log, get_dct_and_bow_from_features, write_csv_and_text_data, ami


def get_args():
    ap = argparse.ArgumentParser(description="Perform latent dirichlet allocation against wiki data")
    ap.add_argument('--num-wikis', dest='num_wikis', type=int,
                    default=os.getenv('NUM_WIKIS', 5000),
                    help="Number of top N wikis to include in learner")
    ap.add_argument('--num-topics', dest='num_topics', type=int,
                    default=os.getenv('NUM_TOPICS', 999),
                    help="Number of topics you want from the LDA process")
    ap.add_argument('--max-topic-frequency', dest='max_topic_frequency', type=int,
                    default=os.getenv('MAX_TOPIC_FREQUENCY', 500),
                    help="Threshold for number of wikis a given topic appears in")
    ap.add_argument('--num-processes', dest="num_processes", type=int,
                    default=os.getenv('NUM_PROCESSES', 8),
                    help="Number of processes for async data access from S3")
    ap.add_argument('--model-prefix', dest='model_prefix', type=str,
                    default=os.getenv('MODEL_PREFIX', datetime.strftime(datetime.now(), '%Y-%m-%d-%H-%M')),
                    help="Prefix to uniqueify model")
    ap.add_argument('--path-prefix', dest='path_prefix', type=str,
                    default=os.getenv('PATH_PREFIX', "/mnt/"),
                    help="Prefix to path")
    ap.add_argument('--s3-prefix', dest='s3_prefix', type=str,
                    default=os.getenv('S3_PREFIX', "models/wiki/"),
                    help="Prefix on s3 for model location")
    ap.add_argument('--auto-launch', dest='auto_launch', type=bool,
                    default=os.getenv('AUTOLAUNCH_NODES', True),
                    help="Whether to automatically launch distributed nodes")
    ap.add_argument('--instance-count', dest='instance_count', type=int,
                    default=os.getenv('NODE_INSTANCES', 20),
                    help="Number of node instances to launch")
    ap.add_argument('--node-ami', dest='ami', type=str,
                    default=os.getenv('NODE_AMI', ami),
                    help="AMI of the node machines")
    ap.add_argument('--dont-terminate-on-complete', dest='terminate_on_complete', action='store_false',
                    default=os.getenv('TERMINATE_ON_COMPLETE', True),
                    help="Prevent terminating this instance")
    ap.add_argument('--git-ref', dest='git_ref',
                    default=os.getenv('GIT_REF', 'master'),
                    help="A DSTK repo ref (tag, branch, commit hash) to check out")
    return ap.parse_args()


def get_data(wiki_id):
    use_caching(per_service_cache={'TopEntitiesService.get': {'dont_compute': True},
                                   'HeadsCountService.get': {'dont_compute': True}})
    hcs = HeadsCountService().get_value(wiki_id)
    tes = TopEntitiesService().get_value(wiki_id)
    if type(hcs) == dict:
        hcs = hcs.items()
    if type(tes) == dict:
        tes = tes.items()
    return wiki_id, {'heads': sorted(hcs, key=lambda y: y[1], reverse=True)[:50],
                     'entities': sorted(tes, key=lambda y: y[1], reverse=True)}


def get_wiki_data_from_api(wiki_ids):
    return dict(requests.get('http://www.wikia.com/api/v1/Wikis/Details',
                             params={'ids': wiki_ids}).json().get('items', {}))


def data_to_features(data_dict):
    features = []
    try:
        heads_to_count = data_dict.get('heads', [])
        entities_to_count = data_dict.get('entities', [])
        api_data = data_dict.get('api_data', {})
        features = []
        features += [word for head, count in heads_to_count for word in [normalize(head)] * int(count)]
        features += [word for entity, count in entities_to_count
                     for word in ['_'.join(filter(lambda x: x, map(normalize, entity.split(' '))))] * int(count)]
        features += unis_bis(api_data.get('title', ''))
        features += unis_bis(api_data.get('headline', ''))
        features += unis_bis(api_data.get('desc', ''))
    except Exception as e:
        log(data_dict)
        print e
        print traceback.format_exc()
    return features


def get_feature_data(args):
    bucket = connect_s3().get_bucket('nlp-data')
    widlines = bucket.get_key('datafiles/topwams.txt').get_contents_as_string().split("\n")
    wids = filter(lambda x: x, widlines)[:args.num_wikis]

    log("Loading entities and heads for ", len(wids), "wikis")
    pool = Pool(processes=args.num_processes)
    r = pool.map_async(get_data, wids)
    r.wait()
    wiki_data = defaultdict(dict, r.get())

    log("Getting data from API")
    widstrings = [','.join(wids[i:i+20]) for i in range(0, len(wids), 20)]
    r = pool.map_async(get_wiki_data_from_api, widstrings)
    for grouping in r.get():
        if type(grouping) != dict:
            continue
        for wiki_id, api_data in grouping.items():
            wiki_data[wiki_id]['api_data'] = api_data

    log("Turning data into features")
    wiki_ids, data_dicts = zip(*wiki_data.items())
    log("Working on", len(wiki_ids))
    r = pool.map_async(data_to_features, data_dicts)
    r.wait()
    wid_to_features = zip(wiki_ids, r.get())
    log(len(set([value for _, values in wid_to_features for value in values])), "features")
    return dict(wid_to_features)


def get_model_from_args(args):
    log("\n---LDA Model---")
    modelname = '%s-%s-lda-%swikis-%stopics.model' % (args.git_ref, args.model_prefix, args.num_wikis, args.num_topics)
    bucket = connect_s3().get_bucket('nlp-data')
    if os.path.exists(args.path_prefix+modelname):
        log("(loading from file)")
        lda_model = gensim.models.LdaModel.load(args.path_prefix+modelname)
    else:
        log(args.path_prefix+modelname, "does not exist")
        key = bucket.get_key(args.s3_prefix+modelname)
        if key is not None:
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
                wid_to_features = get_feature_data(args)
                log("Turning Data into Vectors")
                dct, bow_docs = get_dct_and_bow_from_features(wid_to_features)
                log("Waiting for workers to get sorted out")
                launching.wait()
                log("Waiting an extra five minutes for workers to get their shit together")
                time.sleep(300)
                log("Finally building model from features")
                lda_model = gensim.models.LdaModel(bow_docs.values(),
                                                   num_topics=args.num_topics,
                                                   id2word=dict([(x[1], x[0]) for x in dct.token2id.items()]),
                                                   distributed=True)
                log("Done, saving model.")
                lda_model.save(args.path_prefix+modelname)
                write_csv_and_text_data(args, bucket, modelname, wid_to_features, bow_docs, lda_model)
                log("uploading model to s3")
                key = bucket.new_key('%s%s/%s/%s' % (args.s3_prefix, args.git_ref, args.model_prefix, modelname))
                key.set_contents_from_file(open(args.path_prefix+modelname, 'r'))
                terminate_lda_nodes()
            except Exception as e:
                print e
                print traceback.format_exc()
                terminate_lda_nodes()
                return harakiri()
    return lda_model


def main():
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    args = get_args()
    get_model_from_args(args)
    log("Done")
    if args.terminate_on_complete:
        harakiri()


if __name__ == '__main__':
    main()
