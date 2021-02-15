import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

#TODO
# create process data func 
# copy to the stage 
# create insertion for song and log

def load_staging_tables(cur,conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


# def insert_tables(cur,conn):
#     for query in insert_table_queries:
#         cur.execute(query)
#         conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))
    print(config['CLUSTER']['HOST'])
    cur = conn.cursor()
            
    load_staging_tables(cur, conn)
    conn.commit();
    
#     insert_tables(cur, conn)
    
    conn.close()

if __name__ == "__main__":
    main()