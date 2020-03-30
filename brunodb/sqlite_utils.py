import sqlite3


def get_db(file=None):
    if file:
        db = sqlite3.connect(file, timeout=30.0, check_same_thread=False,
                             isolation_level=None)
        db.execute('pragma journal_mode=wal;')
    else:
        # in memory only
        db = sqlite3.connect(":memory:", timeout=30.0, check_same_thread=False)

    return db


def drop_table(db, table):
    sql = "DROP TABLE IF EXISTS %s" % table
    db.executescript(sql)
    db.commit()


def drop_index(db, index):
    sql = "DROP INDEX IF EXISTS %s" % index
    db.executescript(sql)
    db.commit()


def create_lookup_table(db, table, key_type='TEXT', value_type='TEXT',
                        drop_first=False):
    if drop_first:
        drop_table(db, table)

    sql = "CREATE TABLE {table} (KEY {key_type} PRIMARY KEY, VALUE {value_type} )"

    sql = sql.format(table=table,
                     key_type=key_type,
                     value_type=value_type)
    db.execute(sql)
    db.commit()


def insert_many_hash_map(db, table, values_list):
    db.executemany("INSERT INTO {table} VALUES (?,?)".format(table=table),
                   values_list)
    db.commit()


def key_val_lookup(db, table, key):
    sql = "SELECT VALUE FROM {table} WHERE KEY = '{key}'".format(table=table,
                                                                 key=key)
    result = db.execute(sql).fetchone()
    if result is None:
        return None
    assert len(result) < 2
    return result[0]


def transpose(data):
    keys = data[0].keys()
    return {k: [row[k] for row in data] for k in keys}


def schema_to_schema_string(schema):
    schema_string = ', '.join(list(k + ' ' + v for k, v in schema.items()))
    suffix = ''

    return schema_string + suffix


def list_tables(db):
    return db.execute("select name from sqlite_master where type = 'table'").fetchall()


def get_tables(db):
    return [i['name'] for i in list_tables(db)]


def query_sql(table,
              fields=None,
              join=None,
              join_type='INNER',
              where_extra='',
              count_table_rows=False,
              order_by='serialnumber',
              **kwargs):

    assert join_type.upper() in ['', 'INNER', 'LEFT', 'LEFT OUTER']
    # SQLite supports INNER joins (default) and
    # LEFT OUTER JOINs

    comparators = {'eq': '=',
                   'gt': '>',
                   'lt': '<',
                   'ge': '>=',
                   'le': '<=',
                   'ne': '!='}

    if count_table_rows:
        assert fields is None
        field_string = 'COUNT(*)'
    else:
        if fields is None or fields == '*':
            field_string = '*'
        else:
            field_string = ', '.join(fields)

    if join is not None:
        join_string = '%s JOIN %s ON %s.serialnumber=%s.serialnumber' % (join_type, join, table, join)
    else:
        join_string = ''
    sql = "SELECT %s FROM %s %s" % (field_string, table, join_string)
    vals = []

    where = []
    for key, value in kwargs.items():
        if key == 'serialnumber':
            key = '%s.%s' % (table, key)

        if isinstance(value, tuple):
            comparator = comparators[value[0]]
            value = value[1]
        else:
            comparator = '='

        where.append('%s %s (?)' % (key, comparator))
        vals.append(value)

    if len(where) > 0 or where_extra:
        sql = sql + ' WHERE '

    if len(where) > 0:
        where_string = ' AND '.join(where)
        sql += where_string
    else:
        where_string = ''

    if where_extra:
        if where_string:
            sql += ' AND ' + where_extra
        else:
            sql += ' ' + where_extra

    if isinstance(order_by, str):
        order_by = [order_by]

    order_by_list = []
    for ob in order_by:
        if ob.startswith('-'):
            ob = ob[1:] + ' DESC'

        order_by_list.append(ob)

    # order_by_string = ', '.join(order_by_list)  # NOQA

    # sql += ' ORDER BY %s' % order_by_string
    vals = tuple(vals)

    return sql, vals
