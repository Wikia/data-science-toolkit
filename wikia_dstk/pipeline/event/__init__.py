import json
import os
import requests
from nltk.tokenize import PunktSentenceTokenizer

def chrono_sort(directory):
    """Return a list of files in a directory, sorted chronologically"""
    files = [(os.path.join(directory, filename), os.path.getmtime(os.path.join(directory, filename))) for filename in os.listdir(directory)]
    files.sort(key=lambda x: x[1])
    return files

def ensure_dir_exists(directory):
    """
    Makes sure the directory given as an argument exists, and returns the same
    directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

def clean_list(text):
    """Unsophisticated way of avoiding sentences that might choke the parser."""
    bullet1 = '\xe2\x80\xa2'.decode('utf-8')
    bullet2 = '\xc2\xb7'.decode('utf-8')
    cleaned = []
    for sentence in PunktSentenceTokenizer().tokenize(text):
        if len(sentence.split(' ')) < 50:
            if bullet1 not in sentence and bullet2 not in sentence:
                cleaned.append(sentence.encode('utf-8'))
    return cleaned

class QueryIterator(object):
    """ Options is a dictionary -- use vals(options) on an optparse instance """
    def __init__(self, config, options):
        if type(config) == dict:
            self.host = config["common"]["solr_endpoint"]
        else:
            self.host = config
        if not options.get('query', False):
            raise Exception("Query is required")
        self.configure(config, options)
        self.getMoreDocs()

    def configure(self, config, options):
        self.query = options.get('query')
        self.start = int(options.get('start', 0 ))
        self.firstStart = self.start
        self.rows = options.get('rows', 100)
        self.limit = options.get('limit', None)
        self.docs = []
        self.numFound = None
        self.at = 0
        self.fields = options.get('fields', '*')
        self.filterquery = options.get('filterquery', None)
        self.sort = options.get('sort', None)

    def __iter__(self):
        return self

    def percentLeft(self):
        start = self.firstStart
        max = self.numFound - start if not self.limit else self.limit
        return (float(self.at)/float(max)) * 100

    def getParams(self):
        params = {
             'q': self.query,
            'wt': 'json',
         'start': self.start,
          'rows': self.rows,
            'fl': self.fields
        }
        if self.filterquery:
            params['fq'] = self.filterquery
        if self.sort:
            params['sort'] = self.sort
        return params

    def getMoreDocs(self):
        if self.numFound is not None and self.start >= self.numFound:
            raise StopIteration
        request = requests.get(self.host+"select", params=self.getParams(), timeout=300)
        response = json.loads(request.content)
        self.numFound = response['response']['numFound']
        self.docs = response['response']['docs']
        self.start += self.rows
        return True

    def next(self):
        if self.at == self.limit:
            raise StopIteration
        if (len(self.docs) == 0):
            self.getMoreDocs()
        self.at += 1
        return self.docs.pop()
