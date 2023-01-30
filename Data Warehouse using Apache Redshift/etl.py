import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Run the copy table queries

                Parameters:
                        a (cur): The cursor to the DB connection
                        b (conn): The connection to the DB

                Returns:
                        Nothing.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Run the insert table queries

                Parameters:
                        a (cur): The cursor to the DB connection
                        b (conn): The connection to the DB

                Returns:
                        Nothing.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        -Read the configuration file
        -Make a connection to DB
        -Load the staging tables with the S3 data
        -Insert data in the Fact and Dimension tables
        -Close the conenction
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()