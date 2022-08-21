import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all the tables
    """
    print('DROPING TABLES...')
    for query in drop_table_queries:
        print(query.split()[-1] + ' table droped')
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all the tables
    """
    print('CREATING TABLES...')
    for query in create_table_queries:
        print(query.split()[5] + ' table created')
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    print('FINISHED! Please run etl.py file!')

    conn.close()


if __name__ == "__main__":
    main()
