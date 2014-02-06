"""
Some shared functionality between all of the below scripts
"""

import logging
import numpy as np
import math
from gensim.corpora import Dictionary
from gensim.matutils import corpus2dense
from nltk import SnowballStemmer
from nltk.corpus import stopwords


dictlogger = logging.getLogger('gensim.corpora.dictionary')
stemmer = SnowballStemmer('english')
english_stopwords = stopwords.words('english')


def vec2dense(vector, num_terms):
    """Convert from sparse gensim format to dense list of numbers"""
    return list(corpus2dense([vector], num_terms=num_terms).T[0])


def normalize(phrase):
    global stemmer, english_stopwords
    nonstops_stemmed = [stemmer.stem(token) for token in phrase.split(' ') if token not in english_stopwords]
    return '_'.join(nonstops_stemmed).strip().lower()


class WikiaDSTKDictionary(Dictionary):

    def filter_stops(self, documents, num_stops=300):
        """
        Uses statistical methods  to filter out stopwords
        See http://www.cs.cityu.edu.hk/~lwang/research/hangzhou06.pdf for more info on the algo
        """
        word_probabilities_summed = dict()
        num_documents = len(documents)
        for document in documents:
            doc_bow = self.doc2bow(document)
            sum_counts = sum([float(count) for _, count in doc_bow])
            for token_id, probability in [(token_id, float(count)/sum_counts) for token_id, count in doc_bow]:
                word_probabilities_summed[token_id] = word_probabilities_summed.get(token_id, []) + [probability]
        mean_word_probabilities = [(token_id, sum(probabilities)/num_documents)
                                   for token_id, probabilities in word_probabilities_summed.items()]

        # For variance of probability, using Numpy's variance metric, padding zeroes where necessary.
        # Should do the same job as figure (3) in the paper
        word_statistical_value_and_entropy = [(token_id,
                                               probability   # statistical value
                                               / np.var(probabilities + ([0] * (num_documents - len(probabilities)))),
                                               sum([prob * math.log(1.0/prob) for prob in probability])  # entropy
                                               )
                                              for token_id, probability in mean_word_probabilities]

        # Use Borda counts to combine the rank votes of statistical value and entropy
        sat_ranking = dict(
            map(lambda y: (y[1], y[0]),
                list(enumerate(map(lambda x: x[0],
                                   sorted(word_statistical_value_and_entropy, key=lambda x: x[1])))))
        )
        entropy_ranking = dict(
            map(lambda y: (y[1], y[0]),
                list(enumerate(map(lambda x: x[0],
                                   sorted(word_statistical_value_and_entropy, key=lambda x: x[2])))))
        )
        borda_ranking = sorted([(token_id, entropy_ranking[token_id] + sat_ranking[token_id])
                                for token_id in sat_ranking],
                               key=lambda x: x[1])

        dictlogger.info("keeping %i tokens, removing %i 'stopwords'" %
                        (len(borda_ranking) - num_stops, num_stops))

        # do the actual filtering, then rebuild dictionary to remove gaps in ids
        self.filter_tokens(good_ids=[token_id for token_id, _ in borda_ranking[num_stops:]])
        self.compactify()
        dictlogger.info("resulting dictionary: %s" % self)