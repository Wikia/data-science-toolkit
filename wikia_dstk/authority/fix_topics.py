from .create_database import insert_contrib_data
from . import get_db_and_cursor
from multiprocessing import Pool
from argparse import ArgumentParser, Namespace


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'--host', dest=u'host', default=u'localhost')
    ap.add_argument(u'-u', u'--user', dest=u'user', default=u'root')
    ap.add_argument(u'-p', u'--password', dest=u'password', default=u'root')
    ap.add_argument(u'-d', u'--database', dest=u'database', default=u'authority')
    ap.add_argument(u'-n', u'--num-processes', dest=u'num_processes', type=int, default=6)
    return ap.parse_known_args()


def main():
    args, _ = get_args()
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"SELECT wiki_id FROM wikis")
    namespaces = [Namespace(wid=apply(str, row), **vars(args)) for row in cursor.fetchall()]
    Pool(processes=args.num_processes).map_async(insert_contrib_data, namespaces).get()


if __name__ == u'__main__':
    main()