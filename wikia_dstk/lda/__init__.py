"""
Some shared functionality between all of the below scripts
"""

import logging
import numpy as np
import time
import re
import random
import hashlib
import os
import codecs
from datetime import datetime
from multiprocessing import Pool
from gensim.corpora import Dictionary
from gensim.matutils import corpus2dense
from nltk import PorterStemmer, bigrams, trigrams
from nltk.corpus import stopwords
from collections import defaultdict
from boto import connect_s3
from boto.utils import get_instance_metadata
from boto.ec2 import connect_to_region
from boto.exception import EC2ResponseError
from boto.ec2 import networkinterface
from .. import log, logfile

ami = u"ami-13156323"

ami = u"ami-13156323"


alphanumeric_unicode_pattern = re.compile(ur'[^\w\s]', re.U)
splitter_pattern = ur"[\u200b\s]+"
dictlogger = logging.getLogger(u'gensim.corpora.dictionary')
stemmer = PorterStemmer()
english_stopwords = stopwords.words(u'english')
instances_launched = []
instances_requested = []
connection = None
video_json_key = u'feature-data/video.json'

logfile = u'/var/log/wikia_dstk.lda.log'
log_level = logging.INFO
logger = logging.getLogger(u'wikia_dstk.lda')
logger.setLevel(log_level)
ch = logging.StreamHandler()
ch.setLevel(log_level)
formatter = logging.Formatter(u'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
ch = logging.FileHandler(logfile)
ch.setLevel(log_level)
ch.setFormatter(formatter)
logger.addHandler(ch)


def get_ec2_connection():
    global connection
    if not connection:
        connection = connect_to_region(u'us-west-2')
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


def unis_base(string_or_list):
    if not string_or_list:
        return []
    try:
        totes_list = re.split(splitter_pattern,  string_or_list)  # flags=re.UNICODE if we didn't have 2.6 on nlp-s1
    except AttributeError:
        totes_list = string_or_list  # can't split a list dawg
    unigrams = [normalize(word) for word in totes_list if word]
    unigrams = [u for u in unigrams if u]  # filter empty string
    return unigrams


def unis(string_or_list, prefix=u''):
    return [u'%s%s' % (prefix, word) for word in unis_base(string_or_list)]


def unis_bis(string_or_list, prefix=u''):
    unigrams = unis_base(string_or_list)
    return ([u'%s%s' % (prefix, word) for word in unigrams]
            + [u'%s%s' % (prefix, u'_'.join(gram)) for gram in bigrams(unigrams)])


def unis_bis_tris(string_or_list, prefix=u''):
    unigrams = unis_base(string_or_list)
    return ([u'%s%s' % (prefix, word) for word in unigrams]
            + [u'%s%s' % (prefix, u'_'.join(gram)) for gram in bigrams(unigrams)]
            + [u'%s%s' % (prefix, u'_'.join(gram)) for gram in trigrams(unigrams)])


def get_my_hostname():
    return get_instance_metadata()[u'local-hostname'].split('.')[1]


def get_my_ip():
    return get_instance_metadata()[u'local-ipv4']


def get_my_id():
    return get_instance_metadata()[u'instance-id']


def harakiri():
    """
    Terminate the current instance. Will upload a logfile to s3 if it exists.
    """
    if os.path.exists(logfile):
        b = connect_s3().get_bucket(u'nlp-data')
        k = b.new_key(u'logs/lda/%s-%s.log' % (datetime.strftime(datetime.now(), u'%Y-%m-%d-%H-%M'), get_my_hostname()))
        k.set_contents_from_filename(logfile)
    conn = get_ec2_connection()
    my_id = get_my_id()
    sirs = conn.get_all_spot_instance_requests(filters={'instance-id': my_id})
    sirs[0].cancel()
    conn.terminate_instances(instance_ids=[my_id])


def check_lda_node(instance_request):
    conn = get_ec2_connection()
    fulfilled = False
    while not fulfilled:
        time.sleep(random.randint(10, 20))
        requests = conn.get_all_spot_instance_requests(request_ids=[instance_request.id])
        if len(filter(lambda x: x.status == u'price-too-low', requests)) > 0:
            raise StandardError(u"Bid price too low -- try again later")
        fulfilled = len(filter(lambda x: x.status.code == u'fulfilled', requests)) > 0
    return requests[0].instance_id


def load_instance_ids(instance_ids):
    global instances_launched
    c = get_ec2_connection()
    instances_launched = c.get_all_reservations(instance_ids=instance_ids)
    c.create_tags(instance_ids, {u"Name": u"LDA Worker Node", u"type": u"lda"})


def launch_lda_nodes(instance_count=20, ami=u"ami-f4d0bfc4"):
    global instances_launched, instances_requested
    conn = get_ec2_connection()
    user_data = u"""#!/bin/sh

echo `date` `hostname -i ` "Configuring Environment" >> /var/log/my_startup.log
export PYRO_SERIALIZERS_ACCEPTED=pickle
export PYRO_SERIALIZER=pickle
export PYRO_NS_HOST=%s
echo `date` `hostname -i ` "Starting Worker" >> /var/log/my_startup.log
python -m gensim.models.lda_worker > /var/log/lda_worker 2>&1 &
echo `date` `hostname -i ` "User Data Script Complete" >> /var/log/my_startup.log
""" % get_my_ip()

    instances_requested = conn.request_spot_instances(0.80, ami,
                                                      count=instance_count,
                                                      instance_type=u'm2.4xlarge',
                                                      subnet_id=u'subnet-e4d087a2',
                                                      security_group_ids=['sg-72190a10'],
                                                      user_data=user_data
                                                      )

    return Pool(processes=instance_count).map_async(check_lda_node, instances_requested, callback=load_instance_ids)


def terminate_lda_nodes():
    global instances_launched, instances_requested
    conn = get_ec2_connection()
    try:
        if len(instances_launched):
            conn.terminate_instances(instance_ids=[instance.id
                                                   for reservation in instances_launched
                                                   for instance in reservation.instances])
        if len(instances_requested):
            conn.cancel_spot_instance_requests([r.id for r in instances_requested])
    except EC2ResponseError as e:
        log(e)
        if len(instances_requested):
            conn.cancel_spot_instance_requests([r.id for r in instances_requested])


def log(*args):
    logger.info(u" ".join([unicode(a) for a in args]))


def get_dct_and_bow_from_features(id_to_features):
    log(u"Extracting to dictionary...")
    documents = id_to_features.values()
    dct = WikiaDSTKDictionary(documents)

    log(u"Filtering stopwords")
    dct.filter_stops()

    log(u"Filtering extremes")
    dct.filter_extremes()

    log(u"---Bag of Words Corpus---")
    bow_docs = {}
    for name in id_to_features.keys():
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
    log(u"Writing output and uploading to s3")
    sparse_csv_filename = modelname.replace(u'.model', u'-sparse-topics.csv')
    text_filename = modelname.replace(u'.model', u'-topic-features.csv')
    csv_key = bucket.new_key(u'%s%s/%s/%s' % (args.s3_prefix, args.git_ref, args.model_prefix, sparse_csv_filename))
    text_key = bucket.new_key(u'%s%s/%s/%s' % (args.s3_prefix, args.git_ref, args.model_prefix, text_filename))
    with open(args.path_prefix+sparse_csv_filename, 'w') as sparse_csv:
        for name in id_to_features:
            vec = bow_docs[name]
            sparse = dict(lda_model[vec])
            sparse_csv.write(",".join([str(name)]
                                      + ['%d-%.8f' % (n, sparse.get(n, 0))
                                         for n in range(args.num_topics)
                                         if sparse.get(n, 0) and tally[n] <= args.max_topic_frequency])
                             + "\n")

    csv_key.set_contents_from_file(open(args.path_prefix+sparse_csv_filename, u'r'))

    with codecs.open(args.path_prefix+text_filename, u'w', encoding=u'utf8') as text_output:
        text_output.write(u"\n".join(
            map(lambda x: x.encode(u'utf8', lda_model.show_topics(topics=args.num_topics, topn=15, formatted=True))))
        )

    text_key.set_contents_from_file(open(args.path_prefix+text_filename, u'r'))


def server_user_data_from_args(args, server_model_name, extras=u""):
    return (u"""#!/bin/sh
echo `date` `hostname -i ` "User Data Start" >> /var/log/my_startup.log
mkdir -p /mnt/
cd /home/ubuntu/data-science-toolkit
echo `date` `hostname -i ` "Updating DSTK" >> /var/log/my_startup.log
git fetch origin
git checkout %s
git pull origin %s && sudo python setup.py install
echo `date` `hostname -i ` "Setting Environment Variables" >> /var/log/my_startup.log
export GIT_REF=%s
export NUM_TOPICS=%d
export MAX_TOPIC_FREQUENCY=%d
export MODEL_PREFIX="%s"
export S3_PREFIX="%s"
export NODE_INSTANCES=%d
export NODE_AMI="%s"
export PYRO_SERIALIZERS_ACCEPTED=pickle
export PYRO_SERIALIZER=pickle
export PYRO_NS_HOST="hostname -i"
export ASSIGN_IP="%s"
%s
touch /var/log/lda_dispatcher
touch /var/log/lda_server
chmod 777 /var/log/lda_server
chmod 777 /var/log/lda_dispatcher
echo `date` `hostname -i ` "Starting Nameserver" >> /var/log/my_startup.log
python -m Pyro4.naming -n 0.0.0.0 > /var/log/name_server  2>&1 &
echo `date` `hostname -i ` "Starting Dispatcher" >> /var/log/my_startup.log
python -m gensim.models.lda_dispatcher > /var/log/lda_dispatcher 2>&1 &
echo `date` `hostname -i ` "Running LDA Server Script" >> /var/log/my_startup.log
python -u -m %s > /var/log/lda_server 2>&1 &
echo `date` `hostname -i ` "User Data End" >> /var/log/my_startup.log""" % (
        args.git_ref, args.git_ref, args.git_ref, args.num_topics,
        args.max_topic_frequency, args.model_prefix, args.s3_prefix,
        args.node_count, args.ami, args.master_ip, extras, server_model_name))


def run_server_from_args(args, server_model_name, user_data_extras=""):
    conn = get_ec2_connection()
    try:
        log(u"Running LDA, which will auto-terminate upon completion")

        interface = networkinterface.NetworkInterfaceSpecification(subnet_id='subnet-e4d087a2',
                                                                   groups=['sg-72190a10'],
                                                                   associate_public_ip_address=True)
        interfaces = networkinterface.NetworkInterfaceCollection(interface)

        user_data = server_user_data_from_args(args, server_model_name, user_data_extras)
        reservation = conn.run_instances(args.ami,
                                         instance_type='m2.4xlarge',
                                         user_data=user_data,
                                         network_interfaces=interfaces)
        reso = reservation.instances[0]
        conn.create_tags([reso.id], {u"Name": u"LDA Master Node"})
        while True:
            reso.update()
            print reso.id, reso.state, reso.public_dns_name, reso.private_dns_name
            time.sleep(15)
    except EC2ResponseError as e:
        print e
        if reso:
            conn.terminate_instances([reso.id])
            log(u"TERMINATED MASTER")
    except KeyboardInterrupt:
        if args.killable:
            conn.terminate_instances([reso.id])


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
        return hashlib.sha1(u' '.join(document).encode(u'utf-8')).hexdigest()

    def doc2bow(self, document, allow_update=False, return_missing=False):
        parent = super(WikiaDSTKDictionary, self)
        hsh = self.document2hash(document)
        if allow_update or hsh not in self.d2bmemo:
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

        log(u"Getting probabilities")
        for tupleset in pool.map(get_doc_bow_probs, documents):
            for token_id, prob in tupleset:
                word_probabilities_list[token_id].append(prob)

        log(u"Calculating borda ranking between SAT and entropy")
        wpl_items = word_probabilities_list.items()
        token_ids, probabilities = zip(*wpl_items)
        # padding with zeroes for numpy
        log(u"At probabilities, initializing zero matrix for", len(probabilities), u"tokens")
        sats_and_hs = pool.map(get_sat_h,
                               [(probabilities[i:i+1000], num_documents)
                               for i in range(0, len(probabilities), 10000)])
        log(u'fully calculated')
        token_to_sat = zip(token_ids, [sat for sat_and_h in sats_and_hs for sat in sat_and_h[0]])
        token_to_entropy = zip(token_ids, [sat for sat_and_h in sats_and_hs for sat in sat_and_h[1]])

        dtype = [('token_id', 'i'), ('value', 'f')]
        log(u"Sorting SAT")
        sorted_token_sat = np.sort(np.array(token_to_sat, dtype=dtype), order='value')
        log(u"Sorting Entropy")
        sorted_token_entropy = np.sort(np.array(token_to_entropy, dtype=dtype), order='value')
        log(u"Finally calculating Borda")
        token_to_borda = defaultdict(int)
        for order, tup in enumerate(sorted_token_entropy):
            token_to_borda[tup[0]] += order
        for order, tup in enumerate(sorted_token_sat):
            token_to_borda[tup[0]] += order
        borda_ranking = sorted(token_to_borda.items(), key=lambda x: x[1])

        dictlogger.info(u"keeping %i tokens, removing %i 'stopwords'" %
                        (len(borda_ranking) - num_stops, num_stops))

        # do the actual filtering, then rebuild dictionary to remove gaps in ids
        bad_ids = [token_id for token_id, _ in borda_ranking[:num_stops]]
        self.filter_tokens(bad_ids=bad_ids)
        # we also need to filter the memoized bag of words
        self.d2bmemo = {}
        self.compactify()
        dictlogger.info(u"resulting dictionary: %s" % self)

    def filter_extremes(self, no_below=5, no_above=0.5, keep_n=100000):
        parent = super(WikiaDSTKDictionary, self)
        retval = parent.filter_extremes(no_below=no_below, no_above=no_above, keep_n=keep_n)
        self.d2bmemo = {}
        return retval
