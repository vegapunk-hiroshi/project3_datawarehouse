import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

#TODO flow:
# 0. Check the redshift cluster.
# 1. CREATE FACT and Dimension table, and Staging table
# 2. Use COPY statement to insert the data from s3 into redshift staging table.
# 3. Insert the data from staging table into FACT and Dimension table in the redshift cluster.


def drop_tables(cur, conn):
    """
    - drop all tables if exist.
    
    """
    for query in drop_table_queries:
        
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    - 
    """
    for query in create_table_queries:

        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    print(KEY, '¥n', SECRET)
    
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    
    cur = conn.cursor();

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()