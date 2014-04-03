from . import get_db_and_cursor, add_db_arguments
from argparse import ArgumentParser, Namespace
from multiprocessing import Pool
import requests
import traceback


def get_args():
    ap = add_db_arguments(ArgumentParser())
    ap.add_argument(u'-n', u'--num-processes', dest=u'num_processes', type=int, default=6)
    return ap.parse_known_args()


def get_pageviews_for_wiki(args):
    try:
        db, cursor = get_db_and_cursor(args)
        wiki_id, url = args.row
        cursor.execute(u"SELECT article_id FROM articles WHERE wiki_id = %d" % wiki_id)
        params = {
            u'controller': u'WikiaSearchIndexerController',
            u'method': u'get',
            u'service': u'Metadata'
        }
        print url, cursor.rowcount, u"rows"
        while True:
            rows = cursor.fetchmany(15)
            if not rows:
                break
            params[u'ids'] = u'|'.join([apply(str, x) for x in rows])
            try:
                response = requests.get(u"%swikia.php" % url, params=params).json()
            except ValueError:
                continue
            updates = [(doc[u'id'], doc.get(u"views", {}).get(u"set", 0))
                       for doc in response.get(u"contents", {}) if u'id' in doc]
            if updates:
                cases = u"\n".join([u"WHEN '%s' THEN %d" % update for update in updates])
                update_ids = u"','".join(map(lambda y: str(y[0]), updates))
                sql = u"""
                    UPDATE articles
                    SET pageviews = CASE doc_id
                    %s
                    END
                    WHERE doc_id IN ('%s')""" % (cases, update_ids)
                db.cursor().execute(sql)
                db.commit()
        print u"done with", url
    except Exception as e:
        print e
        print traceback.format_exc()
        raise e


def main():
    args, _ = get_args()
    db, cursor = get_db_and_cursor(args)
    p = Pool(processes=args.num_processes)
    cursor.execute(u"SELECT wiki_id, url FROM wikis ")
    for i in range(0, cursor.rowcount, 500):
        print i
        p.map_async(get_pageviews_for_wiki, [Namespace(row=row, **vars(args)) for row in cursor.fetchmany(500)]).get()




if __name__ == u'__main__':
    main()