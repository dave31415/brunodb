import psycopg2
from psycopg2.extras import RealDictCursor
import psycopg2.extensions
import os


def get_default_config():
    return {'dbname': 'postgres',
            'user': 'postgres',
            'password': None,
            'port': 5432,
            'host': '127.0.0.1'}


def get_connection_string(dbname='postgres', user='postgres', password=None,
                          port=5432, host='127.0.0.1'):
    """
    :param dbname: database name, default 'postgres'
    :param user: username, default 'postgres'
    :param password: password, defaults to environment var POSTGRES_PWD and then empty string
    :param port: port number, default 5432
    :param host: host, default 127.0.0.1
    :return: open connection with an cursor().execute(sql) method
    """
    if password is None:
        password = os.getenv('POSTGRES_PWD')

    if password is None:
        raise ValueError('Password must either by supplied on the function call or placed in'
                         ' environment variable POSTGRES_PWD')

    connection_string = f'dbname={dbname} user={user} password={password} port={port} host={host}'

    return connection_string


def open_connection(dbname='postgres', user='postgres', password=None,
                    port=5432, host='127.0.0.1', db_type=None):
    """
    Open a connection to Postgres. All options have a default except password.
        password can either be supplied or stored in POSTGRES_PWD environment variable.
        Don't hard code it into code or at least in a production setting.
    :param dbname: database name, default 'postgres'
    :param user: username, default 'postgres'
    :param password: password, defaults to environment var POSTGRES_PWD
        raises error if not passed here or found there
    :param port: port number, default 5432
    :param host: host, default 127.0.0.1
    :param db_type: not used here but allows for passing **config
        rather than having to remove it
    :return: open connection with an cursor().execute(sql) method
    """
    assert db_type is None or db_type == 'postgres'
    connection_string = get_connection_string(dbname=dbname, user=user, password=password,
                                              port=port, host=host)

    conn = psycopg2.connect(connection_string)
    return conn


def query_connection(conn, sql, values=None):
    # return generators to rows
    with conn.cursor(cursor_factory=RealDictCursor) as curs:
        curs.execute(sql, vars=values)
        for row in curs:
            yield dict(row)
    conn.commit()


def query_connection_script(conn, sql, values=None):
    with conn.cursor(cursor_factory=RealDictCursor) as curs:
        curs.execute(sql, vars=values)
    conn.commit()


def query_connection_many(conn, sql, values):
    with conn.cursor(cursor_factory=RealDictCursor) as curs:
        curs.executemany(sql, values)
    conn.commit()


def get_tables(conn, include_system=False):

    sql_all = """
        SELECT *
        FROM
        pg_catalog.pg_tables
    """

    sql_non_system = """
            SELECT *
            FROM
            pg_catalog.pg_tables
            WHERE
            schemaname not like 'pg_'
        """

    if include_system:
        sql = sql_all
    else:
        sql = sql_non_system

    system_schemas = ['information_schema', 'pg_catalog']

    result = list(query_connection(conn, sql))

    tables = [row['tablename'] for row in result if row['schemaname'] not in system_schemas]
    return tables


class PostgresDB:
    def __init__(self, config=None):
        self.db_type = 'postgres'
        if config is None:
            config = get_default_config()

        self.config = config
        self.con = open_connection(**config)

    def execute(self, sql, values=None):
        if values is None:
            return query_connection(self.con, sql)
        else:
            return query_connection(self.con, sql, values)

    def executescript(self, sql, values=None):
        query_connection_script(self.con, sql, values=values)

    def executemany(self, sql, values):
        query_connection_many(self.con, sql, values)

    def get_tables(self, include_system=False):
        return get_tables(self.con, include_system=include_system)

    def close(self):
        self.con.close()

    def commit(self):
        self.con.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
