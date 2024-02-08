import logging
import traceback
import json


def read_config(database_type):
    try:
        with open('../config.json', 'r') as file:
            config = json.load(file)
        return config.get(database_type, {})
    except Exception as e:
        logging.fatal(f"Failed to read config file with exception - {e}")
        traceback.print_exc()
        raise SystemExit(1)
