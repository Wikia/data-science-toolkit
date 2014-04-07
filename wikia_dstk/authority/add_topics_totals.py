from argparse import ArgumentParser, Namespace
from . import get_db_and_cursor, add_db_arguments
from multiprocessing import Pool


def get_args():
    ap = add_db_arguments(ArgumentParser())
    ap.add_argument(u'--num-processes', dest=u'num_processes', type=int, default=6)
    return ap.parse_args()


def add_topics_totals(args):
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"""SELECT SUM(IFNULL(arts.global_authority, 0))
                       FROM articles_topics arto
                       INNER JOIN articles arts
                         ON arts.topic_id = %d
                        AND arts.wiki_id = arto.wiki_id
                        AND arts.article_id = arto.article_id
                       """ % args.topic_id)
    (total,) = cursor.fetch()
    cursor.execute(u"""UPDATE topics
                       SET total_authority = %.5f
                       WHERE topic_id = %d""" % (total, args.topic_id))
    db.commit()


def main():
    args = get_args()
    db, cursor = get_db_and_cursor(args)

    cursor.execute(u"""SELECT * FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE TABLE_NAME = 'topics' AND COLUMN_NAME = 'total_authority'""")

    if not cursor.fetchall():
        cursor.execute(u"""ALTER TABLE topics ADD COLUMN total_authority FLOAT NULL""")
        db.commit()

    cursor.execute(u"""SELECT DISTINCT topic_id FROM topics""")

    p = Pool(processes=args.num_processes)
    mp_args = [Namespace(topic_id=row[0], **vars(args)) for row in cursor.fetchall()]
    for i in range(0, len(mp_args), 500):
        print i, u"wikis"
        p.map(add_topics_totals, mp_args[i:i+500])

if __name__ == u"__main__":
    main()