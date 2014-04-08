from argparse import ArgumentParser, Namespace
from . import get_db_and_cursor, add_db_arguments
from multiprocessing import Pool


def get_args():
    ap = add_db_arguments(ArgumentParser())
    ap.add_argument(u'--num-processes', dest=u'num_processes', type=int, default=6)
    return ap.parse_args()


def add_topics_totals(args):
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"""SELECT aru.contribs, arts.local_authority, arts.global_authority
                       FROM articles_users aru
                       INNER JOIN articles arts
                         ON aru.user_id = %d
                        AND arts.wiki_id = aru.wiki_id
                        AND arts.article_id = aru.article_id
                       """ % args.user_id)

    with_contribs = [(row[0] * row[2], row[1] * row[2]) for row in cursor.fetchall() if row and row[1] and row[2]]
    local_auth = sum(map(lambda x: x[0], with_contribs))
    global_auth = sum(map(lambda x: x[1], with_contribs))

    cursor.execute(u"""UPDATE users
                       SET total_authority = %.5f, total_authority_scaled = %.5f
                       WHERE user_id = %d""" % (local_auth, global_auth, args.user_id))
    db.commit()


def main():
    args = get_args()
    db, cursor = get_db_and_cursor(args)

    cursor.execute(u"""SELECT DISTINCT user_id FROM users""")

    print cursor.rowcount, u"user total"
    p = Pool(processes=args.num_processes)
    for i in range(0, cursor.rowcount, 500):
        print i, u"users"
        p.map_async(add_topics_totals,
                    [Namespace(user_id=row[0], **vars(args)) for row in cursor.fetchmany(500)]).get()

if __name__ == u"__main__":
    main()