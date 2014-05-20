import argparse
import json
import requests
import xlwt
import os.path

SOLR = 'http://search-s11:8983/solr/main/select'

ap = argparse.ArgumentParser()
ap.add_argument('--input', '-i', dest='input', action='store',
                help='CSV file to take as input')
ap.add_argument('--dump-json', '-d', dest='dump_json', action='store_true',
                default=False, help='Dump URL and title data to JSON')
ap.add_argument('--load-json', '-l', dest='load_json', action='store_true',
                default=False, help='Load URL and title data from JSON')
args = ap.parse_args()

pids = []
with open(args.input) as c:
    for line in c:
        pids.extend(line.strip().split(','))
pids = list(set(pids))

if args.load_json:
    with open(os.path.join(
        os.path.dirname(args.input),
            'urls-%s.json' % os.path.basename(args.input))) as u:
        urls = json.loads(u.read())
    with open(os.path.join(
        os.path.dirname(args.input),
            'titles-%s.json' % os.path.basename(args.input))) as t:
        titles = json.loads(t.read())
else:
    urls = {}
    titles = {}

    for pid in pids:
        r = requests.get(
            SOLR,
            params={'q': 'id:%s' % pid, 'fl': 'url,title_en', 'wt': 'json'})
        url = ''
        title = ''
        if r.status_code == 200:
            docs = r.json().get('response', {}).get('docs', [])
            if docs:
                doc = docs[0]
                url = doc.get('url', '').encode('utf-8')
                title = doc.get('title_en', '').encode('utf-8')
        print pid, url, title
        urls[pid] = url
        titles[pid] = title

if args.dump_json:
    with open(os.path.join(
        os.path.dirname(args.input),
            'urls-%s.json' % os.path.basename(args.input)), 'w') as u:
        u.write(json.dumps(urls))
    with open(os.path.join(
        os.path.dirname(args.input),
            'titles-%s.json' % os.path.basename(args.input)), 'w') as t:
        t.write(json.dumps(titles))

my_workbook = xlwt.Workbook()

ids_worksheet = my_workbook.add_sheet('Page IDs')
ids_worksheet.write(0, 0, 'Page')
ids_worksheet.write(0, 1, 'Recommendations')

urls_worksheet = my_workbook.add_sheet('URLs')
urls_worksheet.write(0, 0, 'Page')
urls_worksheet.write(0, 1, 'Recommendations')

titles_worksheet = my_workbook.add_sheet('Titles')
titles_worksheet.write(0, 0, 'Page')
titles_worksheet.write(0, 1, 'Recommendations')

with open(args.input) as c:
    for counter, line in enumerate(c.readlines()):
        row = counter + 1
        for col, pid in enumerate(line.strip().split(',')):
            #u.write(','.join(map(lambda x: urls.get(x, ''), pid)) + '\n')
            #t.write(','.join(map(lambda x: titles.get(x, ''), pid)) + '\n')
            ids_worksheet.write(row, col, pid)
            urls_worksheet.write(row, col, urls.get(pid, '').decode('utf-8'))
            titles_worksheet.write(row, col, titles.get(pid, '').decode('utf-8'))

my_workbook.save(args.input.replace('.csv', '.xls'))
