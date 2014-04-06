from . import get_db_and_cursor, MinMaxScaler, add_db_arguments
from argparse import ArgumentParser, Namespace
from multiprocessing import Pool
import traceback


def get_args():
    ap = add_db_arguments(ArgumentParser())
    ap.add_argument(u'-n', u'--num-processes', dest=u'num_processes', type=int, default=6)
    ap.add_argument(u'-s', u'--smoothing', dest=u'smoothing', type=float, default=0.0001)
    return ap.parse_known_args()


def scale_authority_pv(args):
    db, cursor = get_db_and_cursor(args)
    cursor.execute(u"SELECT wam_score FROM wikis WHERE wiki_id = %d" % args.wiki_id)
    wam = cursor.fetchone()[0]
    cursor.execute(u"SELECT MAX(pageviews), MIN(pageviews) FROM articles WHERE wiki_id = %d" % args.wiki_id)
    max_pv, min_pv = cursor.fetchone()
    sql = (u"""UPDATE articles
               SET local_authority_pv = local_authority
                                      * ((pageviews - %0.5f)/(1-%0.5f)) + %0.5f)"""
           % (min_pv, (max_pv - min_pv), + args.smoothing))
    cursor.execute(sql)
    db.commit()

    mms = MinMaxScaler(set_min=0, set_max=100, enforced_min=1, enforced_max=10)
    cursor.execute(u"""UPDATE articles
                       SET global_authority = local_authority_pv * %d WHERE wiki_id = %d"""
                   % (mms.scale(wam), args.wiki_id))
    db.commit()

    cursor.execute(u"""INSERT INTO topics_users (topic_id, user_id, local_authority_pv, scaled_authority)
                       SELECT arto.topic_id,
                              arus.user_id,
                              arus.contribs * articles.local_authority_pv,
                              arus.contribs * articles.global_authority
                       FROM articles_topics arto
                            INNER JOIN articles
                            ON arto.wiki_id = %d AND articles.wiki_id = %d
                            AND arto.article_id = articles.article_id
                            INNER JOIN articles_users arus
                            ON arus.wiki_id = %d AND arto.wiki_id = %d
                            AND arus.article_id = arto.article_id
                       ON DUPLICATE KEY UPDATE
                       local_authority_pv = IFNULL(local_authority_pv, 0) +  VALUES(local_authority_pv),
                       scaled_authority = IFNULL(scaled_authority_pv, 0) + VALUES(scaled_authority),
                       """
                   % (args.wiki_id, args.wiki_id, args.wiki_id, args.wiki_id))
    db.commit()


def main():
    args, _ = get_args()
    db, cursor = get_db_and_cursor(args)
    p = Pool(processes=args.num_processes)
    cursor.execute(u"SELECT wiki_id FROM wikis ")
    for i in range(0, cursor.rowcount, 500):
        print i, u"wikis"
        p.map_async(scale_authority_pv, [Namespace(wiki_id=row[0], **vars(args)) for row in cursor.fetchmany(500)]).get()


if __name__ == '__main__':
    main()