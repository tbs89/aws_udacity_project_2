import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print("Executing copy:", query)
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Error during copy: {e}")

def insert_tables(cur, conn):
    for query in insert_table_queries:
        print("Executing insert:", query)
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Error during insert: {e}")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = None
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            config.get("CLUSTER", "HOST"),
            config.get("CLUSTER", "DB_NAME"),
            config.get("CLUSTER", "DB_USER"),
            config.get("CLUSTER", "DB_PASSWORD"),
            config.get("CLUSTER", "DB_PORT")))
        cur = conn.cursor()

        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

    except Exception as e:
        print(f"Error connecting to Redshift: {e}")

    finally:
        if conn:
            cur.close()
            conn.close()
            print('ETL process completed and connection closed.')

if __name__ == "__main__":
    main()
