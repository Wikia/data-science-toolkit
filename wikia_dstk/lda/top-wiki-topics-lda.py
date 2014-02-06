import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
import gensim
import os
from nlp_services.discourse.entities import TopEntitiesService
from nlp_services.syntax import HeadsCountService
from nlp_services.caching import use_caching
import sys
import time
from multiprocessing import Pool
from boto import connect_s3
from collections import defaultdict
from . import vec2dense, normalize, WikiaDSTKDictionary

topN = sys.argv[1]

num_topics = int(sys.argv[2])

max_freq = int(sys.argv[3]) if len(sys.argv) > 3 else 500

wids = [str(int(line)) for line in open('topwams.txt').readlines()][:int(topN)]

num_processes = 8  # i'm refactoring you fool
model_prefix = time.time()  # you gettin refactored to b
path_prefix = "/mnt/"  # oprah refactor
s3prefix = "models/wiki/"  # refact ?


def log(*args):
    """
    TODO: use a real logger
    """
    print args


def get_data(wiki_id):
    use_caching(per_service_cache={'TopEntitiesService.get': {'dont_compute': True},
                                   'HeadsCountService.get': {'dont_compute': True}})
    return [(wiki_id, [sorted(HeadsCountService().nestedGet(wid).items(), key=lambda y: y[1], reverse=True)[:50],
                       TopEntitiesService().nestedGet(wiki_id).items()])]


def main():
    log("Loading entities and heads...")
    entities = []

    r = Pool(processes=num_processes).map_async(get_data, wids)
    r.wait()
    entitites = dict(r.get())

    widToEntityList = defaultdict(list)
    for wid in entities:
        for heads_to_count, entities_to_count in entities[wid]:
            widToEntityList[wid] += [word for head, count in heads_to_count for word in [normalize(head)] * count]
            widToEntityList[wid] += [word for entity, count in entities_to_count for wor in [normalize(entity)] * count]

    log(len(widToEntityList), "wikis")
    log(len(set([value for values in widToEntityList.values() for value in values])), "features")

    log("Extracting to dictionary...")

    dct = WikiaDSTKDictionary(widToEntityList.values())
    unfiltered = dct.token2id.keys()
    dct.filter_stops()
    filtered = dct.token2id.keys()

    log("---Bag of Words Corpus---")

    bow_docs = {}
    for name in widToEntityList:
        sparse = dct.doc2bow(widToEntityList[name])
        bow_docs[name] = sparse
        dense = vec2dense(sparse, num_terms=len(dct))

    log("\n---LDA Model---")

    modelname = '%d-lda-%swikis-%stopics.model' % (model_prefix, sys.argv[1], sys.argv[2])

    built = False
    bucket = connect_s3().get_bucket('nlp-data')
    if os.path.exists(path_prefix+modelname):
        log("(loading from file)")
        lda_model = gensim.models.LdaModel.load(path_prefix+modelname)
    else:
        log(path_prefix+modelname, "does not exist")
        key = bucket.get_key(s3prefix+modelname)
        if key is not None:
            log("(loading from s3)")
            with open('/tmp/modelname', 'w') as fl:
                key.get_contents_to_file(fl)
            lda_model = gensim.models.LdaModel.load('/tmp/modelname')
        else:
            built = True
            log("(building... this will take a while)")
            lda_model = gensim.models.LdaModel(bow_docs.values(),
                                               num_topics=num_topics,
                                               id2word=dict([(x[1], x[0]) for x in dct.token2id.items()]),
                                               distributed=True)
            log("Done, saving model.")
            lda_model.save(path_prefix+modelname)

    # counting number of features so that we can filter
    tally = defaultdict(int)
    for name in widToEntityList:
        vec = bow_docs[name]
        sparse = lda_model[vec]
        for (feature, frequency) in sparse:
            tally[feature] += 1


    # Write to sparse_csv here, excluding anything exceding our max frequency
    log("Writing topics to sparse CSV")
    sparse_csv_filename = '%d-%swiki-%stopics-sparse-topics.csv' % (model_prefix, sys.argv[1], sys.argv[2])
    text_filename = '%d-%swiki-%stopics-topic_names.text' % (model_prefix, sys.argv[1], sys.argv[2])
    with open(path_prefix+sparse_csv_filename, 'w') as sparse_csv:
        for name in widToEntityList:
            vec = bow_docs[name]
            sparse = dict(lda_model[vec])
            sparse_csv.write(",".join([str(name)]
                                      + ['%d-%.8f' % (n, sparse.get(n, 0))
                                         for n in range(num_topics)
                                         if tally[n] < max_freq])
                             + "\n")

    with open(path_prefix+text_filename, 'w') as text_output:
        text_output.write("\n".join(lda_model.show_topics(topics=args.num_topics, topn=15, formatted=True)))

    log("Uploading data to S3")
    csv_key = bucket.new_key(s3prefix+sparse_csv_filename)
    csv_key.set_contents_from_file(path_prefix+sparse_csv_filename)
    text_key = bucket.new_key(s3prefix+text_filename)
    text_key.set_contents_from_file(path_prefix+text_filename)

    log("Done")

    if built:
        log("uploading model to s3")
        key = bucket.new_key(modelname)
        key.set_contents_from_file(modelname)


if __name__ == '__main__':
    main()
