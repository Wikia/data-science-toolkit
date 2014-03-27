from argparse import ArgumentParser, Namespace
from . import filter_wids
from boto import connect_s3
from multiprocessing import Pool
from nlp_services.caching import use_caching
from nlp_services.authority import WikiAuthorityService
import os
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
    u""")

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
    u""")

    print u"\tCreating table users..."
    cursor.execute(u"""
    CREATE TABLE  users (
      user_id INT PRIMARY KEY NOT NULL,
      username varchar(255) NOT NULL,
      total_authority FLOAT NULL
    ) ENGINE=InnoDB
    u""")

    print u"\tCreating table topics..."
    cursor.execute(u"""
    CREATE TABLE  topics (
      topic_id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
      name VARCHAR(255) NOT NULL,
      total_authority FLOAT NULL,
      UNIQUE KEY (name)
    ) ENGINE=InnoDB
    u""")

    print u"\tCreating table articles_users..."
    cursor.execute(u"""
    CREATE TABLE  articles_users (
      doc_id VARCHAR(255) NOT NULL,
      user_id INT NOT NULL,
      authority FLOAT NOT NULL,
      UNIQUE KEY (doc_id, user_id)
    ) ENGINE=InnoDB
    u""")

    print u"\tCreating table topics_users..."
    cursor.execute(u"""
    CREATE TABLE topics_users (
      topic_id INT NOT NULL,
      user_id INT NOT NULL,
      authority FLOAT NULL,
      FOREIGN KEY (topic_id) REFERENCES topics(topic_id),
      FOREIGN KEY (user_id) REFERENCES users(user_id),
      UNIQUE KEY (topic_id, user_id)
    ) ENGINE= InnoDB
    u""")

    print u"\tCreating table articles_topics..."
    cursor.execute(u"""
    CREATE TABLE articles_topics (
      topic_id INT NOT NULL,
      article_id INT NOT NULL,
      wiki_id INT NOT NULL,
      authority FLOAT NULL,
      FOREIGN KEY (topic_id) REFERENCES topics(topic_id),
      FOREIGN KEY (article_id) REFERENCES articles(article_id),
      FOREIGN KEY (wiki_id) REFERENCES wikis(wiki_id),
      UNIQUE KEY (topic_id, wiki_id, article_id)
    ) ENGINE= InnoDB
    u""")

    print u"Created all tables"


def insert_data(args):
    use_caching(is_read_only=True, shouldnt_compute=True)
    db = get_db_connection(args)
    cursor = db.cursor()
    cursor.execute(u"USE authority")

    print u"Inserting wiki data for", args.wid

    items = requests.get(u'http://www.wikia.com/api/v1/Wikis/Details', params={u'ids': args.wid}).json().get(u'items')
    if not items:
        return False

    wiki_data = items[args.wid]

    cursor.execute(u"""
    INSERT INTO wikis (wiki_id, wam_score, title, url) VALUES (%s, %s, u"%s", u"%s")
    u""" % (args.wid, str(wiki_data[u'wam_score']).encode(u'utf8'),
            wiki_data[u'title'].encode(u'utf8'), wiki_data[u'url'].encode(u'utf8')))

    authority_dict = WikiAuthorityService().get_value(args.wid)
    if not authority_dict:
        return False

    print u"Inserting authority data for pages on wiki", args.wid
    for key in authority_dict:
        splt = key.split(u'_')
        wiki_id, article_id = splt[-2], splt[-1]   # fix stupid bug
        cursor.execute(u"""
        INSERT INTO articles (doc_id, article_id, wiki_id, local_authority) VALUES (u"%s", %s, %s, %s)
        u""" % (key, article_id, wiki_id, str(authority_dict[key])))


def get_db_connection(args):
    return mdb.connect(args.host, args.user, args.password)


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