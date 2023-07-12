import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Executes a list of queries to drop tables in Redshift.

    This function iterates through a list of SQL DROP TABLE queries,
    executing each one and committing the results to the database.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor.
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Executes a list of queries to create tables in Redshift.

    This function iterates through a list of SQL CREATE TABLE queries,
    executing each one and committing the results to the database.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor.
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
