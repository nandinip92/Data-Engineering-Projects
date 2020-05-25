import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import re


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
        conn.set_session(autocommit=True)
    except psycopg2.Error as e:     
        print("Error: Could not make connection to the Postgres database")    
        print(e)
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:     
        print("Error: Could not get curser to the Database")    
        print(e) 
    
    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:       
        print(e)
    try:
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
        print("Created sparkifydb Successfully....!!!")
    except psycopg2.Error as e:       
        print(e)
    

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:  
        print("Error: Could not make connection to 'sparkifydb' Postgres database")
        print(e)
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:  
        print("Error: Could not make connection to 'sparkifydb' Postgres database")
        print(e)
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print(e)
            
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            table_name = re.findall(r'EXISTS\ (.+?)\ \(',query)
            print("'{}' table created successfully...!!!".format(table_name[0]))
        except psycopg2.Error as e:
            print(e)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()
    print()


if __name__ == "__main__":
    main()