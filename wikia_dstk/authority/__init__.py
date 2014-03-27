import requests
from multiprocessing import Pool
from boto import connect_s3


def exists(wid):
    return wid, requests.get('http://www.wikia.com/api/v1/Wikis/Details',
                             params=dict(ids=[wid.strip()])).json().get('items')


def not_processed(wid):
    bucket = connect_s3().get_bucket('nlp-data')
    return wid, not bucket.get_key('service_responses/%s/WikiAuthorityService.get' % wid.strip())


def filter_wids(wids, refresh=False):
    p = Pool(processes=8)
    wids = [x[0] for x in p.map_async(exists, wids).get() if x[1]]
    if not refresh:
        wids = [x[0] for x in p.map_async(not_processed, wids).get() if x[1]]

    return wids
