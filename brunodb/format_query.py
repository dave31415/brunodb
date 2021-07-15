import string
from psycopg2 import sql


def format_sql_postgres(sql_template, param_dict):
    # the returned query has as_text method or can be executed
    params = {k: sql.Identifier(v) for k, v in param_dict.items()}
    query = sql.SQL(sql_template).format(**params)
    return query


def check_word(word):
    allowed_chars = set(string.ascii_letters).union({'.', '_'})
    for char in word:
        if char not in allowed_chars:
            raise ValueError('Char %s not allowed in parameter names')


def format_query_check_chars(sql, values_dict):
    # May not be completely secure against sql injections
    # but prevents most. Far better than nothing.
    params = {}
    for k, v in values_dict.items():
        check_word(v)
        params[k] = v

    return sql.format(**params)


def format_sql_in_context(sql_template, param_dict, conn):
    """
    A generic formatter that should work for both SQLite
    and Postgres
    :param sql_template: template string
    :param param_dict: params dict
    :param conn: a postgres connection or None for Sqlite
    :return:
    """
    if conn is not None:
        # Postgres, secure
        query = format_sql_postgres(sql_template, param_dict)
        return query.as_string(conn)

    # sqlite, may not be perfectly secure, better than nothing
    return format_query_check_chars(sql_template, param_dict)
