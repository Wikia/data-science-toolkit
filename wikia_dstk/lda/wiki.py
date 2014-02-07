import time
import warnings
import os
import argparse
warnings.filterwarnings('ignore', category=DeprecationWarning)
import gensim
from nlp_services.discourse.entities import TopEntitiesService
from nlp_services.syntax import HeadsCountService
from nlp_services.caching import use_caching
from multiprocessing import Pool
from boto import connect_s3
from collections import defaultdict
from . import normalize, WikiaDSTKDictionary


def log(*args):
    """
    TODO: use a real logger
    """
    print args


def get_data(wiki_id):
    use_caching(per_service_cache={'TopEntitiesService.get': {'dont_compute': True},
                                   'HeadsCountService.get': {'dont_compute': True}})
    return [(wiki_id, [sorted(HeadsCountService().get_value(wiki_id).items(), key=lambda y: y[1], reverse=True)[:50],
                       TopEntitiesService().get_value(wiki_id).items()])]


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
    ap.add_argument('--wamids-file', dest='wamids_file', type=argparse.FileType,
                    default=os.getenv('WAMIDS_FILE', 'topwams.txt'),
                    help="File listing for top WAM wikis by WAM descending")  # I want an API for this, yo
    ap.add_argument('--num-processes', dest="num_processes", type=int,
                    default=os.getenv('NUM_PROCESSES', 8),
                    help="Number of processes for async data access from S3")
    ap.add_argument('--model-prefix', dest='model_prefix', type=str,
                    default=os.getenv('MODEL_PREFIX', time.time()),
                    help="Prefix to uniqueify model")
    ap.add_argument('--path-prefix', dest='path_prefix', type=str,
                    default=os.getenv('PATH_PREFIX', "/mnt/"),
                    help="Prefix to path")
    ap.add_argument('--s3-prefix', dest='s3_prefix', type=str,
                    default=os.getenv('S3_PREFIX', "models/wiki/"),
                    help="Prefix on s3 for model location")
    return ap.parse_args()


def main():

    args = get_args()
    wids = [str(int(ln)) for ln in args.wamids_file.readlines()][args.num_wikis]

    log("Loading entities and heads...")
    r = Pool(processes=args.num_processes).map_async(get_data, wids)
    r.wait()
    entities = dict(r.get())

    wid_to_features = defaultdict(list)
    for wid in entities:
        for heads_to_count, entities_to_count in entities[wid]:
            wid_to_features[wid] += [word for head, count in heads_to_count for word in [normalize(head)] * count]
            wid_to_features[wid] += [word for entity, count in entities_to_count
                                     for word in [normalize(entity)] * count]

    log(len(wid_to_features), "wikis")
    log(len(set([value for values in wid_to_features.values() for value in values])), "features")

    log("Extracting to dictionary...")

    documents = wid_to_features.values()
    dct = WikiaDSTKDictionary(documents)
    dct.filter_stops(documents)

    log("---Bag of Words Corpus---")

    bow_docs = {}
    for name in wid_to_features:
        sparse = dct.doc2bow(wid_to_features[name])
        bow_docs[name] = sparse

    log("\n---LDA Model---")

    modelname = '%d-lda-%swikis-%stopics.model' % (args.model_prefix, args.num_wikis, args.num_topics)

    built = False
    bucket = connect_s3().get_bucket('nlp-data')
    if os.path.exists(args.path_prefix+modelname):
        log("(loading from file)")
        lda_model = gensim.models.LdaModel.load(args.path_prefix+modelname)
    else:
        log(args.path_prefix+modelname, "does not exist")
        key = bucket.get_key(args.s3_prefix+modelname)
        if key is not None:
            log("(loading from s3)")
            with open('/tmp/modelname', 'w') as fl:
                key.get_contents_to_file(fl)
            lda_model = gensim.models.LdaModel.load('/tmp/modelname')
        else:
            built = True
            log("(building... this will take a while)")
            lda_model = gensim.models.LdaModel(bow_docs.values(),
                                               num_topics=args.num_topics,
                                               id2word=dict([(x[1], x[0]) for x in dct.token2id.items()]),
                                               distributed=True)
            log("Done, saving model.")
            lda_model.save(args.path_prefix+modelname)

    # counting number of features so that we can filter
    tally = defaultdict(int)
    for name in wid_to_features:
        vec = bow_docs[name]
        sparse = lda_model[vec]
        for (feature, frequency) in sparse:
            tally[feature] += 1

    # Write to sparse_csv here, excluding anything exceding our max frequency
    log("Writing topics to sparse CSV")
    sparse_csv_filename = '%d-%swiki-%stopics-sparse-topics.csv' % (args.model_prefix, args.num_wikis, args.num_topics)
    text_filename = '%d-%swiki-%stopics-topic_names.text' % (args.model_prefix, args.num_wikis, args.num_topics)
    with open(args.path_prefix+sparse_csv_filename, 'w') as sparse_csv:
        for name in wid_to_features:
            vec = bow_docs[name]
            sparse = dict(lda_model[vec])
            sparse_csv.write(",".join([str(name)]
                                      + ['%d-%.8f' % (n, sparse.get(n, 0))
                                         for n in range(args.num_topics)
                                         if tally[n] < args.max_topic_frequency])
                             + "\n")

    with open(args.path_prefix+text_filename, 'w') as text_output:
        text_output.write("\n".join(lda_model.show_topics(topics=args.num_topics, topn=15, formatted=True)))

    log("Uploading data to S3")
    csv_key = bucket.new_key(args.s3_prefix+sparse_csv_filename)
    csv_key.set_contents_from_file(args.path_prefix+sparse_csv_filename)
    text_key = bucket.new_key(args.s3_prefix+text_filename)
    text_key.set_contents_from_file(args.path_prefix+text_filename)

    log("Done")

    if built:
        log("uploading model to s3")
        key = bucket.new_key(modelname)
        key.set_contents_from_file(modelname)


if __name__ == '__main__':
    main()
