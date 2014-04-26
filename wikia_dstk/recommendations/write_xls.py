import requests
import sys
import xlwt

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
    r = requests.get(
        SOLR, params={'q': 'id:%s' % pid, 'fl': 'url,title_en', 'wt': 'json'})
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

with open(csv) as c:
    for counter, line in enumerate(c.readlines()):
        row = counter + 1
        for col, pid in enumerate(line.strip().split(',')):
            #u.write(','.join(map(lambda x: urls.get(x, ''), pid)) + '\n')
            #t.write(','.join(map(lambda x: titles.get(x, ''), pid)) + '\n')
            ids_worksheet.write(row, col, pid)
            urls_worksheet.write(row, col, urls.get(pid, ''))
            titles_worksheet.write(row, col, titles.get(pid, ''))

my_workbook.save(csv.replace('.csv', '.xls'))
