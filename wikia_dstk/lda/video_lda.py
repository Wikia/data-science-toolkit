import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
import argparse
import os
import gensim
import time
from multiprocessing import Pool
from . import WikiaDSTKDictionary
from boto import connect_s3
from collections import defaultdict


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
    return parser.parse_args()


def get_data(line):
    split = line.split('\t')
    split_filtered = filter(lambda y: y != '' and len(y.split('~')) > 1 and y.split('~')[1].strip() != '', split[1:])
    return [(split[0], split_filtered)]


def log(*args):
    # todo real logging
    log(args)


def main():
    args = get_args()

    log("Loading terms...")

    pool = Pool(processes=args.num_processes)
    r = pool.map_async(get_data, args.datafile)
    doc_id_to_terms = dict(r.get())

    log(len(doc_id_to_terms), "instances")
    
    log("Extracting to dictionary...")
    documents = doc_id_to_terms.values()
    dct = WikiaDSTKDictionary(documents)
    dct.filter_stops(documents)
    
    log("Bag of Words Corpus...")
    bow_docs = {}
    for doc_id in doc_id_to_terms:
        bow_docs[doc_id] = dct.doc2bow(doc_id_to_terms[doc_id])
    
    log("\n---LDA Model---")
    lda_docs = {}
    modelname = '%s-video-%dtopics.model' % (args.model_prefix, args.num_topics)
    model_location = args.path_prefix+'/'+modelname
    built = False
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
            # todo -- load up slave instances
            log("(building...)")
            lda_model = gensim.models.LdaModel(bow_docs.values(),
                                               num_topics=args.num_topics,
                                               id2word=dict([(x[1], x[0]) for x in dct.token2id.items()]),
                                               distributed=True)
            log("Done, saving model.")
            lda_model.save(model_location)
            built = True

    tally = defaultdict(int)
    for name in doc_id_to_terms:
        vec = bow_docs[name]
        sparse = lda_model[vec]
        for (feature, frequency) in sparse:
            tally[feature] += 1

    log("Writing topics to files")
    sparse_filename = args.path_prefix+modelname.replace('.model', '-sparse-topics.csv')
    text_filename = args.path_prefix+modelname.replace('.model', '-topic-words.txt')
    with open(sparse_filename, 'w') as sparse_csv:
        for doc_id in doc_id_to_terms:
            vec = bow_docs[doc_id]
            sparse = lda_model[vec]
            lda_docs[doc_id] = sparse
            sparse_csv.write(",".join([str(doc_id)]+['%d-%.8f' % x for x in sparse])+"\n")
    
    with open(text_filename, 'w') as text_output:
        text_output.write("\n".join(lda_model.show_topics(topics=args.num_topics, topn=15, formatted=True)))

    log("Uploading data to S3")
    csv_key = bucket.new_key(args.s3_prefix+sparse_filename)
    csv_key.set_contents_from_file(args.path_prefix+sparse_filename)
    text_key = bucket.new_key(args.s3_prefix+text_filename)
    text_key.set_contents_from_file(args.path_prefix+text_filename)

    log("Done")

    if built:
        log("uploading model to s3")
        key = bucket.new_key(args.s3_prefix+modelname)
        key.set_contents_from_file(args.path_prefix+modelname)
    
    log("Done")


if __name__ == '__main__':
    main()