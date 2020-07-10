import configparser
import psycopg2
import logging as log
import re
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        Drops all the tables if exists
    """
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print(e)


def create_tables(cur, conn):
    """
        Creates all required tables
    """

    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
            table_name = re.findall(r"EXISTS\ (.+?)\ \(", query)
            print("'{}' table created successfully...!!!".format(table_name[0]))
    except psycopg2.Error as e:
        print(e)


def main():
    """
        - Gets database details from the configuration file `dwh.cfg`
        - Extablishes connection with the sparkify database and gets cursor to it
        - Drops the tables if exists
        - Create tables
        - Finally closes the connection
    """
    config = configparser.ConfigParser()
    config.read("cloud.cfg")

    print("Connecting to Redshift Cluster...")
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()
    print("Connected to Redshift Cluster...")

    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()
    
    print("******* SCRIPT COMPLETED *******")


if __name__ == "__main__":
    main()
