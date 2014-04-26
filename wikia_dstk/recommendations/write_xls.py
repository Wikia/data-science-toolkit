import json
import requests
import sys

SOLR = 'http://search-s11:8983/solr/main/select'

csv = sys.argv[1]

pids = []
with open(csv) as c:
    for line in c:
        pids.extend(line.strip().split(','))
pids = list(set(pids))

urls = {}
titles = {}

for pid in pids:
    r = requests.get(SOLR, params={'q': 'id:%s' % pid, 'fl': 'url,title_en', 'wt': 'json'})
    url = ''
    title = ''
    #print r.content
    if r.status_code == 200:
        docs = r.json().get('response', {}).get('docs', [])
        if docs:
            doc = docs[0]
            url = doc.get('url', '').encode('utf-8')
            title = doc.get('title_en', '').encode('utf-8')
    print pid, url, title
    urls[pid] = url
    titles[pid] = title

with open('urls-%s.json' % csv, 'w') as u:
    u.write(json.dumps(urls))
with open('titles-%s.json' % csv, 'w') as t:
    t.write(json.dumps(titles))

with open(csv) as c:
    with open('urls-%s' % csv, 'w') as u:
        with open('titles-%s' % csv, 'w') as t:
            for line in c:
                pids = line.strip().split(',')
                u.write(','.join(map(lambda x: urls.get(x, ''), pids)) + '\n')
                t.write(','.join(map(lambda x: titles.get(x, ''), pids)) + '\n')
