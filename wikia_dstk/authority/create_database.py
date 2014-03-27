from argparse import ArgumentParser, Namespace
from . import filter_wids
from boto import connect_s3
from multiprocessing import Pool
from nlp_services.caching import use_caching
from nlp_services.authority import WikiAuthorityService, PageAuthorityService
from nlp_services.discourse.entities import WikiPageToEntitiesService
import os
import json
import traceback
import time
import requests
import MySQLdb as mdb


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'--host', dest=u'host', default=u'localhost')
    ap.add_argument(u'-u', u'--user', dest=u'user', default=u'root')
    ap.add_argument(u'-p', u'--password', dest=u'password', default=u'root')
    ap.add_argument(u'-d', u'--database', dest=u'database', default=u'authority')
    ap.add_argument(u'-s', u'--s3path', dest=u's3path', default=u'datafiles/topwams.txt')
    ap.add_argument(u'-w', u'--no-wipe', dest=u'wipe', default=True, action=u'store_false')
    ap.add_argument(u'-n', u'--num-processes', dest=u'num_processes', default=6)
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
      UNIQUE KEY (article_id, user_id, wiki_id)
    ) ENGINE=InnoDB
    """)

    print u"\tCreating table topics_users..."
    cursor.execute(u"""
    CREATE TABLE topics_users (
      topic_id INT NOT NULL,
      user_id INT NOT NULL,
      local_authority FLOAT NULL,
      scaled_authority FLOAT NULL,
      FOREIGN KEY (topic_id) REFERENCES topics(topic_id),
      FOREIGN KEY (user_id) REFERENCES users(user_id),
      UNIQUE KEY (topic_id, user_id)
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
      UNIQUE KEY (topic_id, wiki_id, article_id)
    ) ENGINE= InnoDB
    """)

    print u"Created all tables"


def insert_data(args):
    try:
        use_caching(is_read_only=True, shouldnt_compute=True)
        db = get_db_connection(args)
        cursor = db.cursor()
        cursor.execute(U'USE authority')

        print u"Inserting wiki data for", args.wid

        response = requests.get(u'http://www.wikia.com/api/v1/Wikis/Details',
                                params={u'ids': args.wid})

        items = response.json().get(u'items')
        if not items:
            return False

        wiki_id = args.wid
        wiki_data = items[args.wid]

        cursor.execute(u"""
        INSERT INTO wikis (wiki_id, wam_score, title, url) VALUES (%s, %s, "%s", "%s")
        """ % (args.wid, str(wiki_data[u'wam_score']),
               wiki_data[u'title'], wiki_data[u'url']))
        db.commit()

        authority_dict = WikiAuthorityService().get_value(args.wid)
        if not authority_dict:
            return False

        print u"Inserting authority data for pages on wiki", args.wid
        authority_dict_fixed = dict([(key.split(u'_')[-2]+u'_'+key.split(u'_')[-1], val)
                                     for key, val in authority_dict.items()])

        wpe = WikiPageToEntitiesService().get_value(wiki_id)
        if not wpe:
            print u"NO WIKI PAGE TO ENTITIES SERVICE FOR", wiki_id
            return False

        print u"Priming entity data"
        for page, entity_data in wpe.items():
            entity_list = list(set(entity_data.get(u'redirects', {}).values() + entity_data.get(u'titles')))
            for entity in entity_list:
                cursor.execute(u"""
                INSERT IGNORE INTO topics (name) VALUES ("%s")
                """ % entity)

        db.commit()

        print u"Inserting page and author and contrib data for wiki", wiki_id
        for doc_id in authority_dict_fixed:
            wiki_id, article_id = doc_id.split(u'_')
            cursor.execute(u"""
            INSERT INTO articles (doc_id, article_id, wiki_id, local_authority) VALUES ("%s", %s, %s, %s)
            """ % (doc_id, article_id, wiki_id, str(authority_dict_fixed[key])))

            entity_data = wpe.get(article_id, {})
            entity_list = list(set(entity_data.get(u'redirects', {}).values() + entity_data.get(u'titles')))
            cursor.execute(u"""
            SELECT id FROM topics WHERE name IN ("%s")
            """ % (u'", "'.join(entity_list)))
            topic_ids = []
            for result in cursor.fetchall():
                topic_ids.append(result[0])
                cursor.execute(u"""
                INSERT INTO articles_topics (article_id, wiki_id, topic_id) VALUES (%s, %s, %s)
                """ % (article_id, wiki_id, result[0]))
            db.commit()

            cursor = db.cursor()

            for contribs in PageAuthorityService().get_value(doc_id, []):
                cursor.execute(u"""
                INSERT IGNORE INTO users (user_id, user_name) VALUES (%s, "%s")
                """ % (contribs[u'user_id'].decode(u'utf8'), contribs[u'user'].decode(u'utf8')))

                cursor.execute(u"""
                INSERT IGNORE INTO articles_users (article_id, wiki_id, user_id, contribs) VALUES (%s, %s, "%s", %s)
                """ % (article_id, wiki_id, contribs[u'user_id'].decode(u'utf8'), contribs[u'contribs']))

                local_authority = contribs[u'contribs'] * authority_dict_fixed.get(page, 0)
                for topic_id in topic_ids:
                    cursor.execute(u"""
                    INSERT INTO topics_users (user_id, topic_id, local_authority) VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE local_authority = local_authority + %s
                    """ % (contribs[u'user_id'].decode(u'utf8'), topic_id, local_authority, local_authority))
                db.commit()
        db.commit()

    except Exception as e:
        print e, traceback.format_exc()
        raise e


def get_db_connection(args):
    return mdb.connect(host=args.host, user=args.user, passwd=args.password,
                       use_unicode=True, charset=u'utf8')


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
    p.map_async(insert_data, [Namespace(wid=wid, **vars(args)) for wid in wids]).get()
    print u"Finished in", (time.time() - start), u"seconds"


if __name__ == u'__main__':
    main()