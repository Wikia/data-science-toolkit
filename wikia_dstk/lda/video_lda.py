import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
import requests
import argparse
import os, sys
import gensim
from sklearn.svm import SVC
from multiprocessing import Pool

parser = argparse.ArgumentParser(description='Generate a per-page topic model using latent dirichlet analysis.')
parser.add_argument('--data_file', dest='datafile', nargs='?', type=argparse.FileType('r'),
                    help="The source file of video features")
parser.add_argument('--num_topics', dest='num_topics', type=int, action='store',
                    help="The number of topics for the model to use")
parser.add_argument('--model_dest', dest='model_dest', type=str, action='store',
                    help="Where to save the model")
args = parser.parse_args()


def get_data(line):
    split = line.split('\t')
    split_filtered = filter(lambda x: x != '' and len(x.split('~')) > 1 and x.split('~')[1].strip() != '', split[1:])
    return [(split[0], split_filtered)]


def vec2dense(vec, num_terms):
    """Convert from sparse gensim format to dense list of numbers"""
    return list(gensim.matutils.corpus2dense([vec], num_terms=num_terms).T[0])

print "Loading terms..."
doc_id_to_terms_tuples = []
pool = Pool(processes=8)
for result in pool.map(get_data, args.datafile):
    doc_id_to_terms_tuples += result

doc_id_to_terms = dict(doc_id_to_terms_tuples)
print len(doc_id_to_terms), "instances"

print "Extracting to dictionary..."
dct = gensim.corpora.Dictionary(doc_id_to_terms.values())
dct.filter_extremes()

print "Bag of Words Corpus..."
bow_docs = {}
for doc_id in doc_id_to_terms:
    bow_docs[doc_id] = dct.doc2bow(doc_id_to_terms[doc_id])

print "\n---LDA Model---"
lda_docs = {}
modelname = 'video-%dtopics.model' % (args.num_topics)
model_location = args.model_dest+'/'+modelname
if os.path.exists(model_location):
    print "(loading from file)"
    lda_model = gensim.models.LdaModel.load(model_location)
else:
    print model_location, "does not exist"
    print "(building...)"
    lda_model = gensim.models.LdaModel(bow_docs.values(),
                                       num_topics=args.num_topics,
                                       id2word=dict([(x[1], x[0]) for x in dct.token2id.items()]),
                                       distributed=True)
    print "Done, saving model."
    lda_model.save(model_location)


print "Writing topics to files"
sparse_filename = args.model_dest+'/video-%dtopics-sparse-topics.csv' % (args.num_topics)
dense_filename = args.model_dest+'/video-%dtopics-dense-topics.csv' % (args.num_topics)
text_filename = args.model_dest+'/video-%dtopics-words.txt' % (args.num_topics)
with open(sparse_filename, 'w') as sparse_csv:
    with open(dense_filename, 'w') as dense_csv:
        for doc_id in doc_id_to_terms:
            vec = bow_docs[doc_id]
            sparse = lda_model[vec]
            dense = vec2dense(sparse, args.num_topics)
            lda_docs[doc_id] = sparse
            sparse_csv.write(",".join([str(doc_id)]+['%d-%.8f' % x for x in sparse])+"\n")
            dense_csv.write(",".join([doc_id]+['%.8f' % x for x in list(dense)])+"\n")

with open(text_filename, 'w') as text_output:
    text_output.write("\n".join(lda_model.show_topics(topics=args.num_topics, topn=15, formatted=True)))

print "Done"
