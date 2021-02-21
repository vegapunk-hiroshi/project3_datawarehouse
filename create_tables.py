import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    - drop all tables if exist.
    """
    for query in drop_table_queries:
        print('*'*30)
        cur.execute(query)
        conn.commit()
        print('table dropped', query)
        


def create_tables(cur, conn):
    """
    - create staging table and analytical table in redshift cluster
    """
    for query in create_table_queries:
        print('*'*30)
        cur.execute(query)
        conn.commit()
        print('table created', query)


def main():
    """"
    - parse the .cfg file for connecting to the redshift cluster
    - get cursor of the cluster
    - drop table if exists
    - create staging table and analytical table in redshift cluster
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print('*'*30)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('connected to redshift cluster')
    
    cur = conn.cursor();

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()