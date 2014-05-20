from __future__ import division
from argparse import ArgumentParser
from datetime import datetime, timedelta
from ... import ensure_dir_exists
import requests


EVENT_DIR = ensure_dir_exists('/data/events/')
LAST_INDEXED = '/data/last_indexed.txt'
SPLIT = 4  # Number of event files to split the time delta into


def get_args():
    """
    Generate configuration options from command-line arguments.

    :rtype: argparse.Namespace
    :return: Options for writing queries
    """
    parser = ArgumentParser()
    parser.add_argument(
        '-q', '--query', dest='query', default=None, help='The query to write')
    parser.add_argument(
        '-w', '--wids', dest='wids', default=None,
        help='A list of wiki IDs to write queries for')
    parser.add_argument(
        '-l', '--last-indexed', dest='last_indexed', action='store_true',
        default=False,
        help='Write a query for all documents indexed since last time')
    parser.add_argument(
        '-a', '--all-wikis', dest='all_wikis', default=False, action='store_true',
        help="Use this script to write events for all English wikis with 50 or more articles.")
    return parser.parse_args()


def total_seconds(td):
    """
    Return the total number of seconds in a datetime.timedelta object
    Explicitly defined, since official implementation is missing from
    datetime.timedelta before Python 2.7

    :type td: datetime.timedelta
    :param td: A timedelta object for which to calculate # of seconds

    :rtype: float
    :return: The number of seconds in the timedelta object
    """
    return ((td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) /
            10**6)


def write_since_last_indexed():
    """
    Write query for all English documents indexed since last date stored,
    splitting into multiple event files to facilitate multiprocessing.
    """
    # Read the date this script was last run
    with open(LAST_INDEXED, 'r') as f:
        last_indexed = datetime.strptime(f.read().strip(),
                                         '%Y-%m-%dT%H:%M:%S.%f')

    # Set the current date
    now = datetime.utcnow()

    # Split the time delta into equal parts
    delta = total_seconds(now - last_indexed)
    increment = delta / SPLIT
    delimiters = [(last_indexed + timedelta(seconds=increment*i)) for i in
                  range(SPLIT)] + [now]

    # Write the current date to file for future use
    with open(LAST_INDEXED, 'w') as f:
        f.write(datetime.isoformat(now))

    # Write multiple Solr queries to the events directory
    for i in range(SPLIT):
        from_ = datetime.isoformat(delimiters[i])
        to = datetime.isoformat(delimiters[i+1])
        query = 'iscontent:true AND lang:en AND indexed:[%sZ TO %sZ]' % (from_,
                                                                         to)
        with open(EVENT_DIR + '%d' % i, 'w') as f:
            f.write(query)


def write_for_all_wikis():
    """
    Writes queries for all English wikis with 50 or more articles
    """
    params = {'q': 'lang_s:en AND articles_i:[50 TO *]', 'wt': 'json', 'rows': 500, 'fl': 'id', 'start': 0}
    while True:
        # can't use search-s10 here because of bad data, i hear there's a team whose job it is to handle that
        response = requests.get('http://search-s9:8983/solr/xwiki/select', params=params).json()
        map(lambda x: write_for_wid(x['id']), response['response']['docs'])
        if params['start'] + params['rows'] >= response['response']['numFound']:
            return
        params['start'] += params['rows']


def write_wids(wids):
    """
    Write queries for all wiki IDs in a given file.

    :type wids: string
    :param wids: Path to a file containing wiki IDs separated by newlines
    """
    with open(wids) as wids_list:
        [write_for_wid(wid) for wid in wids_list]


def write_for_wid(wid):
    """
    Writes an event file for each wiki ID

    :type wid: int
    :param wid: wiki ID
    """
    wid = wid.strip()
    query = 'iscontent:true AND wid:%s' % wid
    with open(EVENT_DIR + wid, 'w') as f:
        f.write(query)



def write_query(query):
    """
    Write a given query.

    :type query: string
    :param query: The exact query to write
    """
    now = datetime.isoformat(datetime.utcnow())
    with open(EVENT_DIR + now, 'w') as f:
        f.write(query)


def main():
    args = get_args()
    if args.query is not None:
        write_query(args.query)
    elif args.all_wikis is not None:
        write_for_all_wikis()
    elif args.wids is not None:
        write_wids(args.wids)
    elif args.last_indexed:
        write_since_last_indexed()


if __name__ == '__main__':
    main()
