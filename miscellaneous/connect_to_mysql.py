

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
