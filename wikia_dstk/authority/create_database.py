from argparse import ArgumentParser, Namespace
from . import filter_wids, get_db_connection, get_db_and_cursor, add_db_arguments
from boto import connect_s3
from multiprocessing import Pool
from nlp_services.caching import use_caching
from nlp_services.authority import WikiAuthorityService, PageAuthorityService
from nlp_services.discourse.entities import WikiPageToEntitiesService
import os
import traceback
import time
import requests


def get_args():
    ap = add_db_arguments(ArgumentParser())
    ap.add_argument(u'-s', u'--s3path', dest=u's3path', default=u'datafiles/topwams.txt')
    ap.add_argument(u'-w', u'--no-wipe', dest=u'wipe', default=True, action=u'store_false')
    ap.add_argument(u'-n', u'--num-processes', dest=u'num_processes', type=int, default=6)
    return ap.parse_known_args()


def create_tables(args):
    db = get_db_connection(args)
    cursor = db.cursor()

    print u"Creating tables"

    if args.wipe:
        cursor.execute(u"DROP DATABASE IF EXISTS authority")

    cursor.execute(u"""CREATE DATABASE IF NOT EXISTS authority 
                            DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;""")
    cursor.execute(u"USE authority")

    print u"\tCreating table wikis..."
    cursor.execute(u"""
    CREATE TABLE  wikis (
      wiki_id INT PRIMARY KEY NOT NULL,
      wam_score FLOAT NULL,
      title VARCHAR(255) NULL,
      url VARCHAR(255) NULL,
      authority FLOAT NULL
    ) ENGINE=InnoDB
    """)

    print u"\tCreating table articles..."
    cursor.execute(u"""
    CREATE TABLE  articles (
      doc_id varchar(255) PRIMARY KEY NOT NULL,
      article_id INT NOT NULL,
      wiki_id INT NOT NULL,
      pageviews INT NULL,
      local_authority FLOAT NULL,
      local_authority_pv FLOAT NULL,
      global_authority FLOAT NULL,
      FOREIGN KEY (wiki_id) REFERENCES wikis(wiki_id),
      UNIQUE KEY (article_id, wiki_id)
    ) ENGINE=InnoDB
    """)

    print u"\tCreating table users..."
    cursor.execute(u"""
    CREATE TABLE  users (
      user_id INT PRIMARY KEY NOT NULL,
      user_name varchar(255) NOT NULL,
      total_authority FLOAT NULL,
      total_authority_scaled FLOAT NULL
    ) ENGINE=InnoDB
    """)

    print u"\tCreating table topics..."
    cursor.execute(u"""
    CREATE TABLE  topics (
      topic_id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
      name VARCHAR(255) NOT NULL,
      UNIQUE KEY (name)
    ) ENGINE=InnoDB
    """)

    print u"\tCreating table articles_users..."
    cursor.execute(u"""
    CREATE TABLE  articles_users (
      article_id INT NOT NULL,
      wiki_id INT NOT NULL,
      user_id INT NOT NULL,
      contribs FLOAT NOT NULL,
      FOREIGN KEY (article_id) REFERENCES articles(article_id),
      FOREIGN KEY (wiki_id) REFERENCES wikis(wiki_id),
      FOREIGN KEY (user_id) REFERENCES users(user_id),
      PRIMARY KEY (article_id, user_id, wiki_id)
    ) ENGINE=InnoDB
    """)

    print u"\tCreating table topics_users..."
    cursor.execute(u"""
    CREATE TABLE topics_users (
      topic_id INT NOT NULL,
      user_id INT NOT NULL,
      local_authority FLOAT NULL,
      local_authority_pv FLOAT NULL,
      scaled_authority FLOAT NULL,
      FOREIGN KEY (topic_id) REFERENCES topics(topic_id),
      FOREIGN KEY (user_id) REFERENCES users(user_id),
      PRIMARY KEY (topic_id, user_id)
    ) ENGINE= InnoDB
    """)

    print u"\tCreating table articles_topics..."
    cursor.execute(u"""
    CREATE TABLE articles_topics (
      topic_id INT NOT NULL,
      article_id INT NOT NULL,
      wiki_id INT NOT NULL,
      FOREIGN KEY (topic_id) REFERENCES topics(topic_id),
      FOREIGN KEY (article_id) REFERENCES articles(article_id),
      FOREIGN KEY (wiki_id) REFERENCES wikis(wiki_id),
      PRIMARY KEY (topic_id, wiki_id, article_id)
    ) ENGINE= InnoDB
    """)

    print u"Created all tables"


def my_escape(s):
    return s.replace(u'\\', u'').replace(u'"', u'').replace(u"'", u'')


def insert_entities(args):
    try:
        use_caching(is_read_only=True, shouldnt_compute=True)
        db,  cursor = get_db_and_cursor(args)

        wpe = WikiPageToEntitiesService().get_value(args.wid)
        if not wpe:
            print u"NO WIKI PAGE TO ENTITIES SERVICE FOR", args.wid
            return False

        print u"Priming entity data on", args.wid
        for page, entity_data in wpe.items():
            entity_list = map(my_escape,
                              list(set(entity_data.get(u'redirects', {}).values() + entity_data.get(u'titles'))))
            for i in range(0, len(entity_list), 50):
                cursor.execute(u"""
                INSERT IGNORE INTO topics (name) VALUES ("%s")
                """ % u'"), ("'.join(entity_list[i:i+50]))
                db.commit()
        return args
    except Exception as e:
        print e, traceback.format_exc()
        return False


def insert_pages(args):
    try:
        use_caching(is_read_only=True, shouldnt_compute=True)
        db,  cursor = get_db_and_cursor(args)

        authority_dict_fixed = get_authority_dict_fixed(args)
        if not authority_dict_fixed:
            return False

        print u"Inserting authority data for pages on wiki", args.wid

        dbargs = []
        for doc_id in authority_dict_fixed:
                wiki_id, article_id = doc_id.split(u'_')
                dbargs.append((doc_id, article_id, wiki_id, str(authority_dict_fixed[doc_id])))

        cursor.execute(u"""
            INSERT INTO articles (doc_id, article_id, wiki_id, local_authority) VALUES %s
            """ % u", ".join([u"""("%s", %s, %s, %s)""" % arg for arg in dbargs]))

        db.commit()
        return args
    except Exception as e:
        print e, traceback.format_exc()
        return False


def insert_wiki_ids(args):
    try:
        use_caching(is_read_only=True, shouldnt_compute=True)
        db,  cursor = get_db_and_cursor(args)

        print u"Inserting wiki data for", args.wid

        response = requests.get(u'http://www.wikia.com/api/v1/Wikis/Details',
                                params={u'ids': args.wid})

        items = response.json().get(u'items')
        if not items:
            return False

        wiki_data = items[args.wid]

        cursor.execute(u"""
        INSERT INTO wikis (wiki_id, wam_score, title, url) VALUES (%s, %s, "%s", "%s")
        """ % (args.wid, str(wiki_data[u'wam_score']),
               my_escape(wiki_data[u'title']), wiki_data[u'url']))
        db.commit()
        return args
    except Exception as e:
        print e, traceback.format_exc()
        return False


def insert_contrib_data(args):
    try:
        use_caching(is_read_only=True, shouldnt_compute=True)
        db,  cursor = get_db_and_cursor(args)
        wpe = WikiPageToEntitiesService().get_value(args.wid)
        if not wpe:
            print u"NO WIKI PAGE TO ENTITIES SERVICE FOR", args.wid
            return False
        authority_dict_fixed = get_authority_dict_fixed(args)
        if not authority_dict_fixed:
            return False
        print u"Inserting page and author and contrib data for wiki", args.wid
        for doc_id in authority_dict_fixed:
            wiki_id, article_id = doc_id.split(u'_')

            entity_data = wpe.get(doc_id, {})
            entity_list = filter(lambda x: x, map(lambda x: x.strip(), map(my_escape,
                                 list(set(entity_data.get(u'redirects', {}).values()
                                          + entity_data.get(u'titles', []))))))

            cursor.execute(u"""
            SELECT topic_id FROM topics WHERE name IN ("%s")
            """ % (u'", "'.join(entity_list)))
            topic_ids = list(set([result[0] for result in cursor.fetchall()]))

            for topic_id in topic_ids:
                sql = u"""
                INSERT IGNORE INTO articles_topics (article_id, wiki_id, topic_id) VALUES (%s, %s, %s)
                """ % (article_id, wiki_id, topic_id)
                cursor.execute(sql)
                db.commit()

            cursor = db.cursor()

            for contribs in PageAuthorityService().get_value(doc_id, []):
                cursor.execute(u"""
                INSERT IGNORE INTO users (user_id, user_name) VALUES (%d, "%s")
                """ % (contribs[u'userid'], my_escape(contribs[u'user'])))
                db.commit()

                cursor.execute(u"""
                INSERT INTO articles_users (article_id, wiki_id, user_id, contribs) VALUES (%s, %s, %d, %s)
                """ % (article_id, wiki_id, contribs[u'userid'], contribs[u'contribs']))
                db.commit()

                local_authority = contribs[u'contribs'] * authority_dict_fixed.get(doc_id, 0)
                for topic_id in topic_ids:
                    cursor.execute(u"""
                    INSERT INTO topics_users (user_id, topic_id, local_authority) VALUES (%d, %s, %s)
                    ON DUPLICATE KEY UPDATE local_authority = local_authority + %s
                    """ % (contribs[u'userid'], topic_id, local_authority, local_authority))
                    db.commit()
        db.commit()
        print u"Done with", args.wid
        return args
    except Exception as e:
        print e, traceback.format_exc()
        return False


def get_authority_dict_fixed(args):
    authority_dict = WikiAuthorityService().get_value(args.wid)
    if not authority_dict:
        return False

    return dict([(key.split(u'_')[-2]+u'_'+key.split(u'_')[-1], val)
                 for key, val in authority_dict.items()])


def main():
    args, _ = get_args()

    start = time.time()
    create_tables(args)
    bucket = connect_s3().get_bucket(u'nlp-data')
    print u"Getting and filtering wiki IDs"
    if os.path.exists(u'cached_wids'):
        wids = [line.strip() for line in open(u'cached_wids', u'r').readlines() if line.strip()]
    else:
        wids = filter_wids([line.strip()
                            for line in bucket.get_key(args.s3path).get_contents_as_string().split(u"\n")
                            if line.strip()], True)
        open(u'cached_wids', u'w').write(u"\n".join(wids))
    p = Pool(processes=args.num_processes)
    print u"Inserting data"
    pipeline = [insert_wiki_ids, insert_pages, insert_entities, insert_contrib_data]
    wiki_args = [Namespace(wid=wid, **vars(args)) for wid in wids]
    for step in pipeline:
        wiki_args = filter(lambda x: x, p.map_async(step, wiki_args).get())

    print len(wiki_args), u"/", len(wids), u"wikis made it through the pipeline"
    print u"Finished in", (time.time() - start), u"seconds"


if __name__ == u'__main__':
    main()