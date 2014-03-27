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
    ap.add_argument('--host', dest='host', default='localhost')
    ap.add_argument('-u', '--user', dest='user', default='root')
    ap.add_argument('-p', '--password', dest='password', default='root')
    ap.add_argument('-d', '--database', dest='database', default='authority')
    ap.add_argument('-s', '--s3path', dest='s3path', default='datafiles/topwams.txt')
    ap.add_argument('-w', '--no-wipe', dest='wipe', default=True, action='store_false')
    ap.add_argument('-n', '--num-processes', dest='num_processes', default=6)
    return ap.parse_known_args()


def create_tables(args):
    db = get_db_connection(args)
    cursor = db.cursor()

    print "Creating tables"

    if args.wipe:
        cursor.execute("DROP DATABASE IF EXISTS authority")

    cursor.execute("CREATE DATABASE IF NOT EXISTS authority")
    cursor.execute("USE authority")

    print "\tCreating table wikis..."
    cursor.execute("""
    CREATE TABLE  wikis (
      wiki_id INT PRIMARY KEY NOT NULL,
      wam_score FLOAT NULL,
      title VARCHAR(255) NULL,
      url VARCHAR(255) NULL,
      authority FLOAT NULL
    ) ENGINE=InnoDB
    """)

    print "\tCreating table articles..."
    cursor.execute("""
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

    print "\tCreating table users..."
    cursor.execute("""
    CREATE TABLE  users (
      user_id INT PRIMARY KEY NOT NULL,
      username varchar(255) NOT NULL,
      total_authority FLOAT NULL
    ) ENGINE=InnoDB
    """)

    print "\tCreating table topics..."
    cursor.execute("""
    CREATE TABLE  topics (
      topic_id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
      name VARCHAR(255) NOT NULL,
      total_authority FLOAT NULL,
      UNIQUE KEY (name)
    ) ENGINE=InnoDB
    """)

    print "\tCreating table articles_users..."
    cursor.execute("""
    CREATE TABLE  articles_users (
      doc_id VARCHAR(255) NOT NULL,
      user_id INT NOT NULL,
      authority FLOAT NOT NULL,
      UNIQUE KEY (doc_id, user_id)
    ) ENGINE=InnoDB
    """)

    print "\tCreating table topics_users..."
    cursor.execute("""
    CREATE TABLE topics_users (
      topic_id INT NOT NULL,
      user_id INT NOT NULL,
      authority FLOAT NULL,
      FOREIGN KEY (topic_id) REFERENCES topics(topic_id),
      FOREIGN KEY (user_id) REFERENCES users(user_id),
      UNIQUE KEY (topic_id, user_id)
    ) ENGINE= InnoDB
    """)

    print "\tCreating table articles_topics..."
    cursor.execute("""
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
    """)

    print "Created all tables"


def insert_data(args):
    use_caching(is_read_only=True, shouldnt_compute=True)
    db = get_db_connection(args)
    cursor = db.cursor()
    cursor.execute("USE authority")

    print "Inserting wiki data for", args.wid

    items = requests.get('http://www.wikia.com/api/v1/Wikis/Details', params={'ids': args.wid}).json().get('items')
    if not items:
        return False

    wiki_data = items[args.wid]

    cursor.execute("""
    INSERT INTO wikis (wiki_id, wam_score, title, url) VALUES (%s, %s, "%s", "%s")
    """ % (args.wid, str(wiki_data['wam_score']), wiki_data['title'], wiki_data[args.wid]['url']))

    authority_dict = WikiAuthorityService().get_value(args.wid)
    if not authority_dict:
        return False

    print "Inserting authority data for pages on wiki", args.wid
    for key in authority_dict:
        wiki_id, article_id = key.split('_')
        cursor.execute("""
        INSERT INTO articles (doc_id, article_id, wiki_id, local_authority) VALUES ("%s", %s, %s, %s)
        """ % (key, article_id, wiki_id, str(authority_dict[key])))


def get_db_connection(args):
    return mdb.connect(args.host, args.user, args.password)


def main():
    args, _ = get_args()

    start = time.time()
    create_tables(args)
    bucket = connect_s3().get_bucket('nlp-data')
    print "Getting and filtering wiki IDs"
    if os.path.exists('cached_wids'):
        wids = [line.strip() for line in open('cached_wids', 'r').readlines() if line.strip()]
    else:
        wids = filter_wids([line.strip()
                            for line in bucket.get_key(args.s3path).get_contents_as_string().split("\n")
                            if line.strip()], True)
        open('cached_wids', 'w').write("\n".join(wids))
    p = Pool(processes=args.num_processes)
    print "Inserting data"
    map(insert_data, [Namespace(wid=wid, **vars(args)) for wid in wids])
    p.map_async(insert_data, [Namespace(wid=wid, **vars(args)) for wid in wids]).get()
    print "Finished in", (time.time() - start), "seconds"


if __name__ == '__main__':
    main()