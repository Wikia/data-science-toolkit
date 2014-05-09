from argparse import ArgumentParser
from collections import OrderedDict
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
        query = u'(%s) AND is_main_page:true' % u' OR ' .join([u"wid:%i" % wid for wid in wikis.keys()[i:i+100]])
        params = {u'wt': u'json',
                  u'start': i,
                  u'limit': 100,
                  u'q': query,
                  u'fl': u'wid,html_en'}
        for result in requests.get(u'http://search-s10:8983/solr/main/select', params=params).json():
            wikis[result[u'wid']][u'main_page_text'] = result[u'html_en']

    return wikis


def main():
    args = get_args()
    print get_mainpage_text(get_wiki_data())


if __name__ == u'__main__':
    main()