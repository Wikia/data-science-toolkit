import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
import requests
import argparse
import os, sys
import gensim
import json
from flask import Flask
from sklearn.svm import SVC
from multiprocessing import Pool
from nlp_services.document_access import ListDocIdsService
from nlp_services.caching import use_caching
from nlp_services.syntax import WikiToPageHeadsService
from nlp_services.title_confirmation import preprocess
from nlp_services.discourse.entities import WikiPageToEntitiesService

parser = argparse.ArgumentParser(description='Generate a per-page topic model using latent dirichlet analysis.')
parser.add_argument('--wiki_ids', dest='wiki_ids_file', nargs='?', type=argparse.FileType('r'),
                    help="The source file of wiki IDs sorted by WAM")
parser.add_argument('--num_wikis', dest='num_wikis', type=int, action='store',
                    help="The number of wikis to process")
parser.add_argument('--num_topics', dest='num_topics', type=int, action='store',
                    help="The number of topics for the model to use")
parser.add_argument('--model_dest', dest='model_dest', type=str, action='store',
                    help="Where to save the model")
args = parser.parse_args()

use_caching()

def get_data_wid(wid):
    print wid
    use_caching(shouldnt_compute=True)
    #should be CombinedEntitiesService yo
    doc_ids_to_heads = WikiToPageHeadsService().get_value(wid, {})
    doc_ids_to_entities = WikiPageToEntitiesService().get_value(wid, {})
    doc_ids_combined = {}
    if doc_ids_to_heads == {}:
        print wid, "no heads"
    if doc_ids_to_entities == {}:
        print wid, "no entities"
    for doc_id in doc_ids_to_heads:
        entity_response = doc_ids_to_entities.get(doc_id, {'titles': [], 'redirects': {}})
        doc_ids_combined[doc_id] = map(preprocess, 
                                       entity_response['titles']
                                       + entity_response['redirects'].keys()
                                       + entity_response['redirects'].values()
                                       + list(set(doc_ids_to_heads.get(doc_id, []))))
    return doc_ids_combined.items()


def get_data_doc(doc_id):
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


def words2vec(model, wordlist):
    return dict(filter(lambda x: x is not None, [(model.id2word.get(word, None), wordlist.count(word)) for word in set(wordlist)]))


app = Flask(__name__)

@app.route('/doc/<doc_id>/')
def doc(doc_id):
    global lda_model
    data = get_data_doc(doc_id)
    if len(data) == 0:
        return json.dumps({'status': 404, 'message': 'No data for '+doc_id})
    try:
        return json.dumps({doc_id: lda_model[words2vec(lda_model, data)], status: 200})
    except Exception as e:
        return json.dumps({'status': 500, 'message': str(e)})


@app.route('/wiki/wiki_id/')
def wiki(wiki_id):
    global lda_model
    data = get_data_wid(wiki_id)
    if len(data) == 0:
        return json.dumps({'status': 404, 'message': 'No data for '+wiki_id})
    try:
        return json.dumps({'status': 200, wiki_id: dict([(doc_id, words2vec(lda_model, data[doc_id])) for doc_id in data]) })
    except Exception as e:
        return json.dumps({'status': 500, 'message': str(e)})


if __name__ == '__main__':
    modelname = 'page-lda-%dwikis-%dtopics.model' % (args.num_wikis, args.num_topics)
    model_location = args.model_dest+'/'+modelname
    if not os.path.exists(model_location):
        print model_location, "does not exist"
        sys.exit()


    print "\n---Loading LDA Model From File---"
    lda_model = gensim.models.LdaModel.load(model_location)
    print "Done"
    app.run(debug=True, host='0.0.0.0')
