import requests
import MySQLdb as mdb

from multiprocessing import Pool
from boto import connect_s3


def exists(wid):
    return wid, requests.get(u'http://www.wikia.com/api/v1/Wikis/Details',
                             params=dict(ids=[wid.strip()])).json().get(u'items')


def not_processed(wid):
    bucket = connect_s3().get_bucket(u'nlp-data')
    return wid, not bucket.get_key(u'service_responses/%s/WikiAuthorityService.get' % wid.strip())


def filter_wids(wids, refresh=False):
    p = Pool(processes=8)
    wids = [x[0] for x in p.map_async(exists, wids).get() if x[1]]
    if not refresh:
        wids = [x[0] for x in p.map_async(not_processed, wids).get() if x[1]]

    return wids


def add_db_arguments(ap):
    ap.add_argument(u'--host', dest=u'host', default=u'localhost')
    ap.add_argument(u'-u', u'--user', dest=u'user', default=u'root')
    ap.add_argument(u'-p', u'--password', dest=u'password', default=u'root')
    ap.add_argument(u'-d', u'--database', dest=u'database', default=u'authority')
    ap.add_argument(u'-P', u'--port', dest=u'port', type=int, default=None)
    return ap


def get_db_connection(args):
    if args.port:
        return mdb.connect(host=args.host, user=args.user, passwd=args.password, port=args.port,
                           use_unicode=True, charset=u'utf8')
    else:
        return mdb.connect(host=args.host, user=args.user, passwd=args.password,
                           use_unicode=True, charset=u'utf8')


def get_db_and_cursor(args):
    db = get_db_connection(args)
    cursor = db.cursor()
    cursor.execute(U'USE authority')
    return db, cursor


class MinMaxScaler:
    """
    Scales values from 0 to 1 by default
    """

    def __init__(self, vals=None, set_min=None, set_max=None, enforced_min=0, enforced_max=1):
        if vals:
            self.min = min(vals)
            self.max = max(vals)
        else:
            self.min = set_min
            self.max = set_max
        self.enforced_min = enforced_min
        self.enforced_max = enforced_max

    def scale(self, val):
        return (((self.enforced_max - self.enforced_min) * (val - self.min))
                / (self.max - self.min)) + self.enforced_min