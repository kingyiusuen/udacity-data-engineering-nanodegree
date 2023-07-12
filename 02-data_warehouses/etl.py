import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Executes a list of queries to copy data from S3 buckets into Redshift tables.

    This function iterates through a list of SQL COPY queries,
    executing each one and committing the results to the database.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor.
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Executes a list of queries to insert data from staging tables to fact and dimention tables.

    This function iterates through a list of SQL INSERT queries,
    executing each one and committing the results to the database.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor.
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
