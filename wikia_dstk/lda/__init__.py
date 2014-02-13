"""
Some shared functionality between all of the below scripts
"""

import logging
import numpy as np
import math
import time
import re
import random
import hashlib
from multiprocessing import Pool
from gensim.corpora import Dictionary
from gensim.matutils import corpus2dense
from nltk import PorterStemmer, bigrams, trigrams
from nltk.corpus import stopwords
from collections import defaultdict
from boto.ec2 import connect_to_region
from boto.utils import get_instance_metadata


alphanumeric_unicode_pattern = re.compile(ur'[^\w\s]', re.U)
splitter_pattern = ur"[\u200b\s]+"
dictlogger = logging.getLogger('gensim.corpora.dictionary')
stemmer = PorterStemmer()
english_stopwords = stopwords.words('english')
instances_launched = []
connection = None
video_json_key = 'feature-data/video.json'


def get_ec2_connection():
    global connection
    if not connection:
        connection = connect_to_region('us-west-2')
    return connection


def vec2dense(vector, num_terms):
    """Convert from sparse gensim format to dense list of numbers"""
    return list(corpus2dense([vector], num_terms=num_terms).T[0])


def normalize(phrase):
    global stemmer, english_stopwords, alphanumeric_unicode_pattern
    nonstops_stemmed = filter(lambda x: x,
                              [stemmer.stem(token)
                               # flags=re.UNICODE if we didn't have 2.6 on nlp-s1
                               for token in re.split(splitter_pattern,
                                                     re.sub(alphanumeric_unicode_pattern, ' ', phrase))
                               if token and token not in english_stopwords]
                              )
    return u'_'.join(nonstops_stemmed).strip().lower()


def unis_bis_tris(string_or_list, prefix=u''):
    try:
        totes_list = re.split(splitter_pattern,  string_or_list)  # flags=re.UNICODE if we didn't have 2.6 on nlp-s1
    except AttributeError:
        totes_list = string_or_list  # can't split a list dawg
    unis = [normalize(word) for word in totes_list if word]
    return ([u'%s%s' % (prefix, word) for word in unis]
            + [u'%s%s' % (prefix, u'_'.join(gram)) for gram in bigrams(unis)]
            + [u'%s%s' % (prefix, u'_'.join(gram)) for gram in trigrams(unis)])


def harakiri():
    get_ec2_connection().terminate_instances(instance_ids=[get_instance_metadata()['local-hostname'].split('.')[1]])


def check_lda_node(instance_request):
    conn = get_ec2_connection()
    fulfilled = False
    while not fulfilled:
        time.sleep(random.randint(10, 20))
        requests = conn.get_all_spot_instance_requests(request_ids=[instance_request.id])
        if len(filter(lambda x: x.status == 'price-too-low', requests)) > 0:
            raise StandardError("Bid price too low -- try again later")
        fulfilled = len(filter(lambda x: x.status == 'fulfilled', requests)) > 0
    return requests[0].instance_id


def load_instance_ids(instance_ids):
    global instances_launched
    instances_launched = get_ec2_connection().get_all_instances(instance_ids=instance_ids)


def launch_lda_nodes(instance_count=20, ami="ami-40701570"):
    global instances_launched
    conn = get_ec2_connection()
    requests = conn.request_spot_instances('0.80', ami,
                                           count=instance_count,
                                           instance_type='m2.4xlarge',
                                           subnet_id='subnet-e4d087a2',
                                           security_group_ids=['sg-72190a10']
                                           )
    return Pool(processes=instance_count).map_async(check_lda_node, requests, callback=load_instance_ids)


def terminate_lda_nodes():
    global instances_launched
    get_ec2_connection().terminate_instances(instance_ids=[instance.instance_id for instance in instances_launched])


def log(*args):
    """
    TODO: use a real logger
    """
    print args


def get_dct_and_bow_from_features(id_to_features):
    log("Extracting to dictionary...")
    documents = id_to_features.values()
    dct = WikiaDSTKDictionary(documents)

    log("Filtering stopwords")
    dct.filter_stops()

    log("---Bag of Words Corpus---")
    bow_docs = {}
    for name in id_to_features:
        sparse = dct.doc2bow(id_to_features[name])
        bow_docs[name] = sparse
    return dct, bow_docs


def write_csv_and_text_data(args, bucket, modelname, id_to_features, bow_docs, lda_model):
    # counting number of features so that we can filter
    tally = defaultdict(int)
    for name in id_to_features:
        vec = bow_docs[name]
        sparse = lda_model[vec]
        for (feature, frequency) in sparse:
            tally[feature] += 1

    # Write to sparse_csv here, excluding anything exceding our max frequency
    log("Writing topics to sparse CSV")
    sparse_csv_filename = modelname.replace('.model', '-sparse-topics.csv')
    text_filename = modelname.replace('.model', '-topic-features.csv')
    with open(args.path_prefix+sparse_csv_filename, 'w') as sparse_csv:
        for name in id_to_features:
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


def get_sat_h(tup):
    probabilities, matrix_length = tup
    probs_zeros = np.zeros((len(probabilities), matrix_length))
    for i, probs in enumerate(probabilities):
        probs_zeros[i][0:len(probs)] = probs
    probabilities = probs_zeros
    return (np.divide(np.mean(probabilities, axis=1), np.var(probabilities, axis=1)),
            np.nansum(np.multiply(probabilities, np.log(1/probabilities)), axis=1))


def get_doc_bow_probs(doc_bow):
    sum_counts = float(sum([count for _, count in doc_bow]))
    return [(token_id, count/sum_counts) for token_id, count in doc_bow]


class WikiaDSTKDictionary(Dictionary):

    def __init__(self, documents=None):
        self.d2bmemo = {}
        super(WikiaDSTKDictionary, self).__init__(documents=documents)

    def document2hash(self, document):
        return hashlib.sha1(u' '.join(document).encode('utf-8')).hexdigest()

    def doc2bow(self, document, allow_update=False, return_missing=False):
        parent = super(WikiaDSTKDictionary, self)
        hsh = self.document2hash(document)
        if allow_update or hsh not in self.d2bmemo:
            if not allow_update:
                print 'got here'
            self.d2bmemo[hsh] = parent.doc2bow(document, allow_update=allow_update, return_missing=return_missing)
        return self.d2bmemo[hsh]

    def filter_stops(self, num_stops=300):
        """
        Uses statistical methods  to filter out stopwords
        See http://www.cs.cityu.edu.hk/~lwang/research/hangzhou06.pdf for more info on the algo
        """
        pool = Pool(processes=8)
        word_probabilities_list = defaultdict(list)
        documents = self.d2bmemo.values()
        num_documents = len(documents)

        log("Getting probabilities")
        for tupleset in pool.map(get_doc_bow_probs, documents):
            for token_id, prob in tupleset:
                word_probabilities_list[token_id].append(prob)

        log("Calculating borda ranking between SAT and entropy")
        wpl_items = word_probabilities_list.items()
        token_ids, probabilities = zip(*wpl_items)
        # padding with zeroes for numpy
        log("At probabilities, initializing zero matrix for", len(probabilities), "tokens")
        sats_and_hs = pool.map(get_sat_h,
                               [(probabilities[i:i+1000], num_documents)
                               for i in range(0, len(probabilities), 10000)])
        log('fully calculated')
        token_to_sat = zip(token_ids, [sat for sat_and_h in sats_and_hs for sat in sat_and_h[0]])
        token_to_entropy = zip(token_ids, [sat for sat_and_h in sats_and_hs for sat in sat_and_h[1]])

        dtype = [('token_id', 'i'), ('value', 'f')]
        log("Sorting SAT")
        sorted_token_sat = np.sort(np.array(token_to_sat, dtype=dtype), order='value')
        log("Sorting Entropy")
        sorted_token_entropy = np.sort(np.array(token_to_entropy, dtype=dtype), order='value')
        log("Finally calculating Borda")
        token_to_borda = defaultdict(int)
        for order, tup in enumerate(sorted_token_entropy):
            token_to_borda[tup[0]] += order
        for order, tup in enumerate(sorted_token_sat):
            token_to_borda[tup[0]] += order
        borda_ranking = sorted(token_to_borda.items(), key=lambda x: x[1])

        dictlogger.info("keeping %i tokens, removing %i 'stopwords'" %
                        (len(borda_ranking) - num_stops, num_stops))

        # do the actual filtering, then rebuild dictionary to remove gaps in ids
        self.filter_tokens(good_ids=[token_id for token_id, _ in borda_ranking[num_stops:]])
        self.compactify()
        dictlogger.info("resulting dictionary: %s" % self)
