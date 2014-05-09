from argparse import ArgumentParser
from collections import OrderedDict
from textblob import TextBlob
from nltk.util import bigrams
from multiprocessing import Pool
from traceback import format_exc
import requests


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'--num-processes', default=8)
    return ap.parse_args()


def get_wiki_data():
    """
    Gets wiki data as JSON docs
    :return: OrderedDict of search docs, id to doc
    :rtype:class:`collections.OrderedDict`
    """
    params = {u'fl': u'id,top_categories_mv_en,hub_s,top_articles_mv_en,description_txt,sitename_txt',
              u'start': 0,
              u'wt': u'json',
              u'q': u'lang_s:en AND articles_i:[50 TO *]',
              u'rows': 500}
    data = []
    while True:
        response = requests.get(u'http://search-s10:8983/solr/xwiki/select', params=params).json()
        data += response[u'response'][u'docs']
        if response[u'response'][u'numFound'] < params[u'rows'] + params[u'start']:
            return OrderedDict([(d[u'id'], d) for d in data])
        params[u'start'] += params[u'rows']


def get_mainpage_text(wikis):
    """
    Get mainpage text for each wiki
    :param wikis: our wiki data set
    :type wikis:class:`collections.OrderedDict`
    :return: OrderedDict of search docs, id to doc, with mainpage raw text added
    :rtype:class:`collections.OrderedDict`
    """
    for i in range(0, len(wikis), 100):
        query = u'(%s) AND is_main_page:true' % u' OR ' .join([u"wid:%s" % wid for wid in wikis.keys()[i:i+100]])
        params = {u'wt': u'json',
                  u'start': 0,
                  u'rows': 100,
                  u'q': query,
                  u'fl': u'wid,html_en'}
        response = requests.get(u'http://search-s10:8983/solr/main/select', params=params).json()
        for result in response[u'response'][u'docs']:
            wikis[str(result[u'wid'])][u'main_page_text'] = result[u'html_en']

    return wikis


def wiki_to_feature(wiki):
    """
    Specifically handles a single wiki document
    :param wiki: dict for wiki fields
    :type wiki: dict
    :return: tuple with wiki id and list of feature strings
    :rtype: tuple
    """
    try:
        features = []
        bow = []
        features += [u'ORIGINAL_HUB:%s' % wiki.get(u'hub_s', u'')]
        features += [u'TOP_CAT:%s' % c.replace(u' ', u'_').lower() for c in wiki.get(u'top_categories_mv_en', [])]
        bow += [c.replace(u' ', u'_').lower() for c in wiki.get(u'top_categories_mv_en', [])]
        features += [u'TOP_ART:%s' % a.lower().replace(u' ', u'_') for a in wiki.get(u'top_articles_mv_en', [])]
        bow += [a.replace(u' ', u'_').lower() for a in wiki.get(u'top_articles_mv_en', [])]
        desc_ngrams = [u"_".join(n) for grouping in
                       [bigrams(np.lower().split(u' '))
                       for np in TextBlob(wiki.get(u'description_txt', [u''])[0]).noun_phrases]
                       for bigram in grouping
                       for n in bigram]
        bow += desc_ngrams
        features += [u'DESC:%s' % d for d in desc_ngrams]
        bow += bigrams(wiki[u'sitename_txt'][0].lower().split(u' '))
        mp_nps = TextBlob(wiki.get(u'main_page_text', u'')).noun_phrases
        bow += [u"_".join(bg).lower() for bg in [bigrams([n.split(u" ") for n in mp_nps])]]
        bow += [w.lower() for words in [np.split(u" ") for np in mp_nps] for w in words]
        print wiki[u'id']
        return wiki[u'id'], bow + features
    except Exception as e:
        print e, format_exc()
        raise e


def wikis_to_features(args, wikis):
    """
    Turns wikis into a set of features
    :param args:argparse namespace
    :type args:class:`argparse.Namespace`
    :param wikis: our wiki data set
    :type wikis:class:`collections.OrderedDict`
    :return: OrderedDict of features, id to featureset
    :rtype:class:`collections.OrderedDict`
    """
    p = Pool(processes=args.num_processes)
    return OrderedDict(p.map_async(wiki_to_feature, wikis.values()).get())


def main():
    args = get_args()
    features = wikis_to_features(args, get_mainpage_text(get_wiki_data()))


if __name__ == u'__main__':
    main()