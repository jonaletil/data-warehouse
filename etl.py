import configparser
import psycopg2
import pandas as pd
from sql_queries import copy_table_queries, insert_table_queries, test_queries


def load_staging_tables(cur, conn):
    """ 
    Loads data from S3 into the staging tables
    """
    print('Loading S3 files into staging tables...')
    for query in copy_table_queries:
        print('Loading ' + query.split()[1])
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ 
    Process the staging data and populate the fact and dimension tables.
    """
    print('Inserting staging data into main tables...')
    for query in insert_table_queries:
        print('Inserting into ' + query.split()[2])
        cur.execute(query)
        conn.commit()


def run_tests(cur):
    """
    Run test queries on the final dataset for analysis
    """

    print('Run tests...')
    for query in test_queries:
        print(query)
        cur.execute(query)
        rows = cur.fetchall()
        print(pd.DataFrame(rows))


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    run_tests(cur)

    conn.close()


if __name__ == "__main__":
    main()
