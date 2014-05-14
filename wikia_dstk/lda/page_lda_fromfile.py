import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
import argparse
import os
import gensim
from multiprocessing import Pool
from nlp_services.caching import use_caching
from nlp_services.syntax import HeadsService
from nlp_services.title_confirmation import preprocess
from nlp_services.discourse.entities import EntitiesService

parser = argparse.ArgumentParser(description='Generate a per-page topic model using latent dirichlet analysis.')
parser.add_argument('--doc_ids', dest='doc_ids_file', nargs='?', type=argparse.FileType('r'),
                    help="The source file of doc IDs")
parser.add_argument('--num_wikis', dest='num_wikis', type=int, action='store',
                    help="The number of wikis to process")
parser.add_argument('--num_topics', dest='num_topics', type=int, action='store',
                    help="The number of topics for the model to use")
parser.add_argument('--model_dest', dest='model_dest', type=str, action='store',
                    help="Where to save the model")
args = parser.parse_args()

use_caching()


def get_data(doc_id):
    use_caching()
    #should be CombinedEntitiesService yo
    heads = HeadsService().get_value(doc_id, {})
    entities = EntitiesService().get_value(doc_id, {})
    doc_ids_combined = {}
    if doc_ids_to_heads == {}:
        print doc_id, "no heads"
    if doc_ids_to_entities == {}:
        print doc_id, "no entities"
    return entities['titles'].values() + entities['redirects'].keys() + entities['redirects'].values() 


def vec2dense(vec, num_terms):
    """Convert from sparse gensim format to dense list of numbers"""
    return list(gensim.matutils.corpus2dense([vec], num_terms=num_terms).T[0])


print "Loading terms..."
doc_ids = [str(int(doc_id)) for doc_id in args.doc_ids_file]
print "Working on ", len(doc_ids[:args.num_wikis]), "wikis"
doc_id_to_terms_tuples = []
pool = Pool(processes=8)
for result in pool.map(get_data, doc_ids[:args.num_wikis]):
    doc_id_to_terms_tuples += result

doc_id_to_terms = dict(doc_id_to_terms_tuples)
print len(doc_id_to_terms), "instances"

print "Extracting to dictionary..."
dct = gensim.corpora.Dictionary(doc_id_to_terms.values())
dct.filter_extremes(no_below=2)

print "Bag of Words Corpus..."
bow_docs = {}
for doc_id in doc_id_to_terms:
    bow_docs[doc_id] = dct.doc2bow(doc_id_to_terms[doc_id])
    # why this?
    # dense = vec2dense(sparse, num_terms=len(dct))

print "\n---LDA Model---"
lda_docs = {}
modelname = 'page-lda-%dwikis-%dtopics.model' % (args.num_wikis, args.num_topics)
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
                                       distributed=False)
    print "Done, saving model."
    lda_model.save(model_location)


print "Writing topics to files"
sparse_filename = args.model_dest+'/page-lda-%dwiki-%dtopics-sparse-topics.csv' % (args.num_wikis, args.num_topics)
dense_filename = args.model_dest+'%dwiki-%dtopics-dense-topics.csv' % (args.num_wikis, args.num_topics)
text_filename = args.model_dest+'%dwiki-%dtopics-words.txt' % (args.num_wikis, args.num_topics)
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
    text_output.write(lda_model.print_topics(args.num_topics))

print "Done"
