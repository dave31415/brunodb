import string
try:
    from psycopg2 import sql
except ImportError:
    sql = None
    pass


def check_word(input_string):
    # allow list for input string to stop the
    # most obvious SQL injection attacks
    # Is there a better way that works for SQLite?
    # Might possibly block valid inputs.

    letters = set(string.ascii_letters)
    numbers = set(string.digits)
    extra = {'.', '_'}

    allowed_chars = letters.union(numbers).union(extra)

    for char in input_string:
        if char not in allowed_chars:
            raise ValueError('Char %s not allowed in parameter names')


def format_query_check_chars(sql, values_dict):
    # May not be completely secure against sql injections
    # but prevents most. Far better than nothing. Not aware of
    # any vulnerabilities but they might exist.
    params = {}
    for k, v in values_dict.items():
        check_word(v)
        params[k] = v

    return sql.format(**params)


def format_sql_postgres(sql_template, param_dict):
    # the returned query has as_text method or can be executed
    params = {k: sql.Identifier(v) for k, v in param_dict.items()}
    query = sql.SQL(sql_template).format(**params)
    return query


def format_sql_in_context(sql_template, param_dict, conn):
    """
    A generic formatter that should work for both SQLite
    and Postgres. Only 100% secure for Postgres. (By that I mean
    we depend on Postgres' internal security being correct). SQLite
    lacks the internal ability to safely format identifiers like
    table_name, column_name, field_name etc.

    This is to be used for things like tables, columns etc and
    not data-typed values (e.g. numbers or strings).
    Data-typed values can be formatted by internal mechanisms that
    are secured by SQLite or Postgres libraries.

    :param sql_template: template string
    :param param_dict: params dict
    :param conn: a postgres connection or None for Sqlite
    :return:
    """
    if conn is not None:
        # Postgres, secure
        query = format_sql_postgres(sql_template, param_dict)
        return query.as_string(conn)

    # sqlite, may not be perfectly secure, far better than nothing
    return format_query_check_chars(sql_template, param_dict)
