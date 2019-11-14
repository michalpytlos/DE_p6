import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop all tables"""
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all tables"""
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """Create staging and analytics tables on the Redshift cluster"""
    config = configparser.ConfigParser()
    config.read('ct.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
