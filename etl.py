import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import pandas as pd

def load_staging_tables(cur, conn):
    """
    - copying the data from json file into the staging table at redshift
    """
    for query in copy_table_queries:
        print('*'*30)
        cur.execute(query)
        conn.commit()
        print(query, 'copied the data from s3 to staging table in redshift')


def insert_tables(cur, conn):
    """
    - inserting the staging data to analytical data within redshift
    """
    for query in insert_table_queries:
        print('*'*30)
        cur.execute(query)
        conn.commit()
        print(query, 'inserted the data to staging table to fact and dimensional table')

def main():
    """
    - parse the .cfg file for connecting to the redshift cluster
    - get cursor of the cluster
    - load the data from json files to staging table
    - insert the data from staging table to the analytical table
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print('*'*30)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))
    print('connected to redshift cluster')
    

    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    
    insert_tables(cur, conn)
    
    conn.close()

if __name__ == "__main__":
    main()