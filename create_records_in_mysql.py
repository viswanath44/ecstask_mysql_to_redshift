import pandas as pd
import numpy as np
import logging
import traceback
import json
import mysql.connector as connection
from sqlalchemy import create_engine, text


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
        connection_string = f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}"
        mysql_engine = create_engine(connection_string)
        return mysql_engine
    except Exception as e:
        logging.fatal(f"connection to sql failed with exception - {e}")
        traceback.print_exc()
        raise SystemExit(1)


if __name__ == "__main__":
    num_rows = 700000

    engine = connect_to_mysql_db()

    data = {
        "id": np.arange(1, num_rows + 1),
        "name": ["Name_" + str(i) for i in range(1, num_rows + 1)],
        "age": np.random.randint(18, 65, size=num_rows),
        # Add more columns as needed
    }

    df = pd.DataFrame(data)
    with engine.connect() as mysql_connection:
        df.to_sql('dummy3', con=mysql_connection, if_exists='append', index=False, chunksize=1000)
