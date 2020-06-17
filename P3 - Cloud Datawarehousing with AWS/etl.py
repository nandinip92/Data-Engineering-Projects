import configparser
import psycopg2
import re
import pandas as pd
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Function to load the ST (Stage) tables
    """
    for query in copy_table_queries:
        table_name = re.findall(r"COPY\ (.+?)\ from", query)
        try:
            cur.execute(query)
            conn.commit()
            print("'{}' COPY Successful...!!!".format(table_name[0]))
        except psycopg2.Error as e:
            print("Error----->", e)


def update_users_table(cur, conn, user_ids):
    """
        Function to delete the duplicate records in the USERS table
        and insert the latest values of those duplicate user_ids
        (i.e., 'level' update from 'free' to 'paid' )
    """
    max_ts = """ SELECT max_time FROM (SELECT MAX(ts) as max_time, \
                                    userId FROM ST_Events WHERE userId IN {} \
                                    GROUP BY userId ORDER BY userId)""".format(
        user_ids
    )

    updated_level = """ SELECT userId,
                        firstName,
                        lastName,
                        gender, level FROM ST_EVENTS \
                        WHERE ts IN ({}) AND userId IN {}""".format(
        max_ts, user_ids
    )

    cur.execute(updated_level)
    recent_level = cur.fetchall()

    # Deleting all the user_ids which has duplicates
    print("... Deleting duplicates .....")
    delete_query = "DELETE FROM users WHERE user_id IN {}".format(user_ids)
    print(delete_query)
    cur.execute(delete_query)
    conn.commit()

    # Inserting the deleted user_ids with the Most Recent data
    users_insert_query = "INSERT INTO users (user_id, first_name,last_name,gender, level) VALUES  (%s, %s,%s,%s, %s)"
    for row in recent_level:
        cur.execute(users_insert_query, row)
    conn.commit()


def insert_tables(cur, conn):
    """
        Function to load the sparkify tables
    """
    for query in insert_table_queries:
        table_name = re.findall(r"INSERT INTO\ (.+?)\ ", query)
        try:
            cur.execute(query)
            conn.commit()
            print("'{}'  Insert Successful...!!!".format(table_name[0]))
        except psycopg2.Error as e:
            print(e)

    query = """SELECT user_id FROM (SELECT user_id,count(*) c FROM users GROUP BY user_id ) WHERE c>1"""
    u_id = pd.read_sql_query(query, conn)
    user_ids = tuple(u_id["user_id"])
    if user_ids:
        print("Duplicate records for userIds :", user_ids)
        update_users_table(cur, conn, user_ids)
        print("Inserted with the latest information for the updated users")


def main():
    """
        - Establishes connection with the sparkify database from configuration file and gets cursor to it.  
        - Loads the Staging tables
        - Loads other schema tables
        - Finally, closes the connection.
    """
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()
    print("*************** ETL process completed ***************")


if __name__ == "__main__":
    main()
