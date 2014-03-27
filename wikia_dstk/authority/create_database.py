from argparse import ArgumentParser
import MySQLdb as mdb


def get_args():
    ap = ArgumentParser()
    ap.add_argument('--host', dest='host', default='localhost')
    ap.add_argument('-u', '--user', dest='user', default='root')
    ap.add_argument('-p', '--password', dest='pass', default='root')
    ap.add_argument('-d', '--database', dest='database', default='authority')
    ap.add_argument('-s', '--s3file', dest='s3file', default='datafiles/topwams.txt')
    return ap.parse_known_args()


def create_tables(db):
    cursor = db.cursor()

    cursor.execute("CREATE DATABASE IF NOT EXISTS authority")
    cursor.execute("USE authority")

    cursor.execute("""
    CREATE TABLE  wikis (
      wiki_id INT PRIMARY KEY NOT NULL,
      wam_score FLOAT NULL,
      title VARCHAR(255) NULL,
      url VARCHAR(255) NULL,
      authority FLOAT NULL
    ) ENGINE=InnoDB
    """)

    cursor.execute("""
    CREATE TABLE  articles (
      doc_id varchar(255) PRIMARY KEY NOT NULL,
      article_id INT NOT NULL,
      wiki_id INT NOT NULL,
      pageviews INT NULL,
      local_authority FLOAT NULL,
      global_authority FLOAT NULL,
      FOREIGN KEY (wiki_id) REFERENCES wikis(wiki_id)
      UNIQUE KEY (article_id, wiki_id)
    ) ENGINE=InnoDB
    """)

    cursor.execute("""
    CREATE TABLE  users (
      user_id INT PRIMARY KEY NOT NULL,
      username varchar(255) NOT NULL,
      total_authority FLOAT NULL,
    ) ENGINE=InnoDB
    """)

    cursor.execute("""
    CREATE TABLE  topics (
      topic_id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
      name VARCHAR(255) NOT NULL,
      total_authority FLOAT NULL,
    ) ENGINE=InnoDB
    """)

    cursor.execute("""
    CREATE TABLE  articles_users (
      doc_id VARCHAR(255) NOT NULL,
      user_id INT NOT NULL,
      authority FLOAT NOT NULL,
      UNIQUE KEY (doc_id, user_id)
    ) ENGINE=InnoDB
    """)

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




def main():
    args = get_args()
    db_connection = mdb.connect(args.host, args.user, args.password)


if __name__ == '__main__':
    main()