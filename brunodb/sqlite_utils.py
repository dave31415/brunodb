import sqlite3


def get_sqlite_db(filename=None, isolation_level="DEFERRED", journal_mode='OFF'):
    isolation_levels = [None, "DEFERRED", "IMMEDIATE", "EXCLUSIVE"]
    journal_modes = ['DELETE', 'TRUNCATE', 'PERSIST', 'MEMORY', 'WAL', 'OFF']

    if isolation_level is not None:
        isolation_level = isolation_level.upper()

    assert isolation_level in isolation_levels

    journal_mode = journal_mode.upper()
    assert journal_mode in journal_modes

    if filename:
        db = sqlite3.connect(filename,
                             timeout=30.0,
                             check_same_thread=False,
                             isolation_level=isolation_level)

        db.execute('pragma journal_mode=%s;' % journal_mode)
    else:
        # in memory only
        db = sqlite3.connect(":memory:", timeout=30.0, check_same_thread=False)

    return db


def drop_table(db, table_name):
    sql = "DROP TABLE IF EXISTS %s" % table_name
    db.executescript(sql)
    db.commit()


def drop_index(db, index_name):
    sql = "DROP INDEX IF EXISTS %s" % index_name
    db.executescript(sql)
    db.commit()


def create_lookup_table(db, table_name, key_type='TEXT', value_type='TEXT',
                        drop_first=False):
    if drop_first:
        drop_table(db, table_name)

    sql = "CREATE TABLE {table} (KEY {key_type} PRIMARY KEY, VALUE {value_type} )"

    sql = sql.format(table=table_name,
                     key_type=key_type,
                     value_type=value_type)
    db.execute(sql)
    db.commit()


def schema_to_schema_string(schema):
    schema_string = ', '.join(list(k + ' ' + v for k, v in schema.items()))
    suffix = ''

    return schema_string + suffix


def list_tables(db):
    return list(db.execute("select name from sqlite_master where type = 'table'"))


def get_tables(db):
    return [i['name'] for i in list_tables(db)]


class SQLiteDB:
    def __init__(self, filename=None, isolation_level="DEFERRED", journal_mode='OFF'):
        self.db_type = 'sqlite'
        self.db = get_sqlite_db(filename=filename, isolation_level=isolation_level, journal_mode=journal_mode)
        self.db.row_factory = sqlite3.Row

    def execute(self, sql, values=None):
        if values is None:
            return (dict(row) for row in self.db.execute(sql))
        else:
            return (dict(row) for row in self.db.execute(sql, values))

    def executescript(self, sql):
        self.db.executescript(sql)

    def executemany(self, sql, values):
        return (dict(row) for row in self.db.executemany(sql, values))

    def close(self):
        self.db.close()

    def commit(self):
        self.db.commit()

    def get_tables(self):
        return get_tables(self.db)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()


def get_db(filename=None, isolation_level="DEFERRED", journal_mode='OFF'):
    return SQLiteDB(filename=filename, isolation_level=isolation_level, journal_mode=journal_mode)
