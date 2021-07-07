import psycopg2
from psycopg2.extras import RealDictCursor
import psycopg2.extensions
from psycopg2.extensions import connection
import os
import records


def get_connection_string(dbname='postgres', user='postgres', password=None,
                          port=5432, host='127.0.0.1', connection_type='native'):
    """
    :param dbname: database name, default 'postgres'
    :param user: username, default 'postgres'
    :param password: password, defaults to environment var POSTGRES_PWD and then empty string
    :param port: port number, default 5432
    :param host: host, default 127.0.0.1
    :param connection_type: 'native' (psycopg2) or 'records'
    :return: open connection with an cursor().execute(sql) method
    """
    if password is None:
        password = os.getenv('POSTGRES_PWD', '')

    if connection_type == 'native':
        connection_string = f'dbname={dbname} user={user} password={password} port={port} host={host}'
    elif connection_type == 'records':
        connection_string = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    else:
        raise ValueError("connection_type %s must be 'native' or 'records'" % connection_type)

    return connection_string


def open_connection(dbname='postgres', user='postgres', password=None,
                    port=5432, host='127.0.0.1'):
    """
    :param dbname: database name, default 'postgres'
    :param user: username, default 'postgres'
    :param password: password, defaults to environment var POSTGRES_PWD and then empty string
    :param port: port number, default 5432
    :param host: host, default 127.0.0.1
    :return: open connection with an cursor().execute(sql) method
    """
    connection_string = get_connection_string(dbname=dbname, user=user, password=password,
                                              port=port, host=host)

    conn = psycopg2.connect(connection_string)
    return conn


def open_with_records():
    connection_string = get_connection_string(connection_type='records')
    db = records.Database(connection_string)
    return db


def conn_type(conn):
    if isinstance(conn, connection):
        return 'native'
    elif isinstance(conn, records.Database):
        return 'records'
    else:
        raise ValueError('Unknown connection class')


def query_connection(conn, sql):
    # return generators to rows
    with conn.cursor(cursor_factory=RealDictCursor) as curs:
        curs.execute(sql)
        for row in curs:
            yield dict(row)


def query_records(records_db, sql, fetchall=False):
    results = records_db.query(sql, fetchall=fetchall)
    return (r.as_dict() for r in results)


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
            schemaname != 'pg_catalog'
            AND
            schemaname != 'information_schema';
        """

    if include_system:
        sql = sql_all
    else:
        sql = sql_non_system

    if conn_type(conn) == 'records':
        return query_records(conn, sql)
    else:
        return query_connection(conn, sql)


def check_agree():
    cur = open_connection()
    rec = open_with_records()
    tables = list(get_tables(cur, include_system=True))
    tables_rec = list(get_tables(rec, include_system=True))
    assert tables == tables_rec
    print('ok')
