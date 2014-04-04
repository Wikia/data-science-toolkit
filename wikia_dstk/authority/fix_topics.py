from .create_database import insert_contrib_data
from . import get_db_and_cursor, add_db_arguments
from multiprocessing import Pool
from argparse import ArgumentParser, Namespace


def get_args():
    ap = add_db_arguments(ArgumentParser())
    ap.add_argument(u'-n', u'--num-processes', dest=u'num_processes', type=int, default=6)
    return ap.parse_known_args()


def main():
    args, _ = get_args()
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"""SELECT wiki_id  FROM articles_topics
                       GROUP BY wiki_id HAVING COUNT(distinct topic_id) <= 1""")
    namespaces = [Namespace(wid=apply(str, row), **vars(args)) for row in cursor.fetchall()]
    Pool(processes=args.num_processes).map_async(insert_contrib_data, namespaces).get()


if __name__ == u'__main__':
    main()