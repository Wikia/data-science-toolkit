import sys
import requests
from argparse import ArgumentParser, FileType
from . import logger
from collections import OrderedDict
from multiprocessing import Pool


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'--infile', dest=u'infile', type=FileType(u'r'), default=sys.stdin)
    ap.add_argument(u'--outfile', dest=u'outfile', type=FileType(u'w'), default=sys.stdout)
    return ap.parse_args()


def get_wiki_data(wid_group):
    return requests.get(u'http://www.wikia.com/api/v1/Wikis/Details',
                        params={u'ids': u','.join(wid_group)}).json().get(u'items', {})


def main():
    logger.info(u"Expanding Wiki Info...")
    args = get_args()
    wid_to_class = OrderedDict([line.strip().split(',') for line in args.infile])
    p = Pool(processes=8)
    wiki_data = {}
    wids = wid_to_class.keys()
    wid_groups = [wids[i:i+20] for i in range(0, len(wids), 20)]
    map(wiki_data.update, p.map_async(get_wiki_data, wid_groups).get())
    output_lines = []
    for wid, cls in wid_to_class.items():
        wiki_datum = wiki_data.get(wid, {})
        output_lines.append(u",".join([wid, cls, wiki_datum.get(u'url', u''), wiki_datum.get(u'title', u''), ]))
    args.outfile.write(u"\n".join(output_lines))


if __name__ == u'__main__':
    main()