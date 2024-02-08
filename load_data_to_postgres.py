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


# connecting to mysql database using mysql.connector
def connect_to_mysql_db():
    config = read_config('mysql')

    host = config.get('hostname', 'localhost')
    database = config.get('database_name', '')
    username = config.get('username', '')
    password = config.get('password', '')
    port = config.get('port', 0)

    try:

        # mysql_url = f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}"
        # mysql_engine = create_engine(mysql_url)
        # return mysql_engine

        mysqldb = connection.connect(host=host, database=database, user=username, passwd=password, use_pure=True)
        return mysqldb
    except Exception as e:
        logging.fatal(f"connection to sql failed with exception - {e}")
        traceback.print_exc()
        raise SystemExit(1)


"""
    Retrieve data from a specified SQL table using a given cursor.

    Parameters:
    - table_name (str): The name of the SQL table from which to retrieve data.
    - cursor (MySQLCursor): The cursor object connected to the MySQL database.

    Returns:
    - result_dataframe (DataFrame): A Pandas DataFrame containing the retrieved data.
"""


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


"""
    Retrieve data from a specified SQL table in batches using a given cursor.

    Parameters:
    - table_name (str): The name of the SQL table from which to retrieve data.
    - cursor (MySQLCursor): The cursor object connected to the MySQL database.

    Returns:
    - merged_dataframe (DataFrame): A Pandas DataFrame containing the merged data from all batches.
"""


def retrieve_data_from_sql_batching(table_name, cursor):
    try:
        offset = 0
        batch_size = 1000
        dataframes_list = []

        while True:
            query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset};"
            dataframe = pd.read_sql(query, cursor)
            if dataframe.empty:
                break
            dataframes_list.append(dataframe)
            offset += batch_size
            # print(dataframes_list)

        merged_dataframe = pd.concat(dataframes_list, ignore_index=True)
        # print(merged_dataframe)
        cursor.close()
        return merged_dataframe
    except Exception as e:
        logging.error(f"data retrieval from table {table_name} in mysql failed - {e}")
        traceback.print_exc()
        raise SystemExit(1)


"""
    Truncate a specified MySQL table, removing all rows and resetting auto-increment columns.

    Parameters:
    - table_name (str): The name of the MySQL table to be truncated.
    - cursor (MySQLCursor): The cursor object connected to the MySQL database.
"""


def truncate_mysql_table(table_name, cursor):
    try:
        query = f"TRUNCATE TABLE {table_name};"
        my_cursor = cursor.cursor()
        my_cursor.execute(query)
    except Exception as e:
        logging.error(f"truncate table {table_name} in mysql failed - {e}")
        traceback.print_exc()
        raise SystemExit(1)


# connect to postgres database using sqlalchemy
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
        chunk_size = 1000  # modify as per batch_size in mysql
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
    table_data_700k_records = "dummy2"

    table1 = "purchase_order"
    table2 = "transaction"
    table3 = "purchase_record"

    is_truncate = False

    try:
        logging.info("Connecting to mysql database")
        mysql_cursor = connect_to_mysql_db()
        logging.info("connection to sql established successfully")

        table1_df_batching = retrieve_data_from_sql_batching(table1, mysql_cursor)  # testing batching
        # table_data_700k_records_df_batching = retrieve_data_from_sql_batching(table_data_700k_records, mysql_cursor)  # testing batching

        # table1_batching_df = retrieve_data_from_sql_batching(table1, mysql_cursor)
        # logging.info("data from table1 retrieved successfully using batching")
        # table2_batching_df = retrieve_data_from_sql_batching(table2, mysql_cursor)
        # logging.info("data from table2 retrieved successfully using batching")
        # table3_batching_df = retrieve_data_from_sql_batching(table3, mysql_cursor)
        # logging.info("data from table3 retrieved successfully using batching")

        mysql_cursor.close()

        postgres_cursor = connect_to_postgres_db()
        logging.info("connection to postgres established successfully")

        load_in_postgres(table1_df_batching, postgres_cursor, table1)  # testing batching
        # load_in_postgres(table_data_700k_records_df_batching, postgres_cursor, table_data_700k_records)  # testing batching

        # truncate table in mysql after loading in postgres successful
        # if is_truncate is True:
        #     mysql_cursor = connect_to_mysql_db()
        #     truncate_mysql_table("Demotruncate", mysql_cursor)

        """
        Truncate table if data for respective table is loaded in postgres successfully
        """

        # table1_is_loaded = load_in_postgres(table1_batching_df, postgres_cursor, table1)
        # logging.info("data from table1 loaded in postgres successfully")
        # if table1_is_loaded is True:
        #     mysql_cursor = connect_to_mysql_db()
        #     truncate_mysql_table(table1, mysql_cursor)
        #     mysql_cursor.close()
        #
        # table2_is_loaded = load_in_postgres(table2_batching_df, postgres_cursor, table2)
        # logging.info("data from table2 loaded in postgres successfully")
        # if table2_is_loaded is True:
        #     mysql_cursor = connect_to_mysql_db()
        #     truncate_mysql_table(table2, mysql_cursor)
        #     mysql_cursor.close()
        #
        # table3_is_loaded = load_in_postgres(table3_batching_df, postgres_cursor, table3)
        # logging.info("data from table3 loaded in postgres successfully")
        # if table3_is_loaded is True:
        #     mysql_cursor = connect_to_mysql_db()
        #     truncate_mysql_table(table3, mysql_cursor)
        #     mysql_cursor.close()

        display_postgres_data(postgres_cursor, table1)
        postgres_cursor.dispose()
    except Exception as e:
        logging.error(f"Error: {e}")
