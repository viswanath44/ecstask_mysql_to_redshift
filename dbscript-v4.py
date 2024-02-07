import json
import pandas as pd
import mysql.connector as connection
from sqlalchemy import create_engine, text
import logging
import traceback

logging.basicConfig(level=logging.INFO)


def read_config(database_type):
    try:
        with open('config.json', 'r') as file:
            config = json.load(file)
        return config.get(database_type, {})
    except Exception as e:
        logging.fatal(f"Failed to read config file with exception - {e}")
        traceback.print_exc()
        raise SystemExit(1)


def connect_to_mysql_db():
    config = read_config('mysql')

    host = config.get('hostname', 'localhost')
    database = config.get('database_name', '')
    username = config.get('username', '')
    password = config.get('password', '')
    port = config.get('port', 0)

    try:
        mysqldb = connection.connect(host=host, database=database, user=username, passwd=password, use_pure=True)
        return mysqldb
    except Exception as e:
        logging.fatal(f"connection to sql failed with exception - {e}")
        traceback.print_exc()
        raise SystemExit(1)


def retrieve_data_from_sql(table_name, cursor):
    try:
        query = f"Select * from {table_name};"
        result_dataframe = pd.read_sql(query, cursor)
        print(result_dataframe)
        cursor.close()
        return result_dataframe
    except Exception as e:
        logging.error(f"data retrieval from table {table_name} in mysql failed - {e}")
        traceback.print_exc()
        raise SystemExit(1)


def truncate_mysql_table(table_name, cursor):
    try:
        query = f"TRUNCATE TABLE {table_name};"
        my_cursor = cursor.cursor()
        my_cursor.execute(query)
    except Exception as e:
        logging.error(f"truncate table {table_name} in mysql failed - {e}")
        traceback.print_exc()
        raise SystemExit(1)


def retrieve_data_from_sql_batching(table_name, cursor):
    try:
        offset = 0
        batch_size = 2
        dataframes_list = []

        while True:
            query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset};"
            dataframe = pd.read_sql(query, cursor)
            if dataframe.empty:
                break
            dataframes_list.append(dataframe)
            offset += batch_size
            print(dataframes_list)

        merged_dataframe = pd.concat(dataframes_list, ignore_index=True)
        print(merged_dataframe)
        cursor.close()
        return merged_dataframe
    except Exception as e:
        logging.error(f"data retrieval from table {table_name} in mysql failed - {e}")
        traceback.print_exc()
        raise SystemExit(1)


def connect_to_postgres_db():
    config = read_config('postgres')

    host = config.get('hostname', 'localhost')
    database = config.get('database_name', '')
    username = config.get('username', '')
    password = config.get('password', '')
    port = config.get('port', 0)

    try:
        postgres_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        db = create_engine(postgres_url)
        return db
    except Exception as e:
        logging.fatal(f"connection to postgres failed with exception - {e}")
        traceback.print_exc()
        raise SystemExit(1)


def load_in_postgres(df, engine, table_name):
    try:
        chunk_size = 2  # modify as per batch_size
        with engine.connect() as postgres_connection:
            df.to_sql(table_name, con=postgres_connection, if_exists='replace', index=False, chunksize=chunk_size)
        return True
    except Exception as e:
        logging.error(f"data load in postgres failed with exception - {e}")
        traceback.print_exc()
        raise SystemExit(1)


def display_postgres_data(engine, table_name):
    query = f"select * from {table_name};"

    with engine.connect() as postgres_connection:
        result = postgres_connection.execute(text(query))
        print(result.fetchall())


if __name__ == "__main__":
    table1 = "purchase_order"
    table2 = "transaction"
    table3 = "purchase_record"

    is_truncate = True

    try:
        logging.info("Connecting to mysql database")
        mysql_cursor = connect_to_mysql_db()
        logging.info("connection to sql established successfully")

        table1_df_batching = retrieve_data_from_sql_batching(table1, mysql_cursor)  # testing batching

        # table1_df = retrieve_data_from_sql(table1, mysql_cursor)
        # logging.info("data from table1 retrieved successfully")
        # table2_df = retrieve_data_from_sql(table2, mysql_cursor)
        # logging.info("data from table2 retrieved successfully")
        # table3_df = retrieve_data_from_sql(table3, mysql_cursor)
        # logging.info("data from table3 retrieved successfully")

        mysql_cursor.close()

        postgres_cursor = connect_to_postgres_db()
        logging.info("connection to postgres established successfully")

        load_in_postgres(table1_df_batching, postgres_cursor, table1)  # testing batching

        #truncate table in mysql after loading in postgres successful
        if is_truncate is True:
            mysql_cursor = connect_to_mysql_db()
            truncate_mysql_table("Demotruncate", mysql_cursor)

        # load_in_postgres(table1_df, postgres_cursor, table1)
        # logging.info("data from table1 loaded in postgres successfully")
        # load_in_postgres(table2_df, postgres_cursor, table2)
        # logging.info("data from table2 loaded in postgres successfully")
        # load_in_postgres(table3_df, postgres_cursor, table3)
        # logging.info("data from table3 loaded in postgres successfully")

        display_postgres_data(postgres_cursor, table1)
        postgres_cursor.dispose()
    except Exception as e:
        logging.error(f"Error: {e}")
