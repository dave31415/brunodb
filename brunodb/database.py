import sqlite3
import logging
from brunodb.sqlite_utils import get_db, drop_table, get_tables
from brunodb.sqlite_utils import query_sql, list_tables
from brunodb.table import get_table

logger = logging.getLogger(__file__)
database_global = {}


def db_is_open(db):
    try:
        list_tables(db)
    except sqlite3.ProgrammingError:
        return False

    return True


def get_database(database_file):
    global database_global

    if database_file in database_global:
        logger.info('DB found in global')
        db = database_global[database_file]

        if db_is_open(db):
            return db
        else:
            logger.info('Database is closed, reopening')

    logger.info('Getting new database connection')

    db = get_db(database_file)
    database_global[database_file] = db

    return db


class DBase:
    def __init__(self, db_file):
        self.db_file = db_file
        self.db = get_db(self.db_file)
        self.db.row_factory = sqlite3.Row
        self.last_sql = None

        logger.info('Tables: %s' % self.tables.__repr__())

    def query(self, table, count_table_rows=False, **kwargs):
        sql, vals = query_sql(table, count_table_rows=count_table_rows, **kwargs)
        show_sql = False
        if show_sql:
            self.last_sql = sql
            logger.info(sql)
            logger.info(vals.__repr__())

        print(sql)
        cur = self.db.execute(sql, vals)
        if count_table_rows:
            # Just return a number
            result = list(cur)
            assert len(result) == 1
            result = dict(result[0])
            return result['COUNT(*)']

        return (dict(row) for row in cur)

    def raw_sql_query(self, sql, values=None):
        if values is None:
            cur = self.db.execute(sql)
        else:
            cur = self.db.execute(sql, values)

        return (dict(row) for row in cur)

    @property
    def tables(self):
        return get_tables(self.db)

    def lookup(self, table, key_name, key_match, value_name):
        kwargs = {key_name: key_match}
        result = list(self.query(table=table, fields=[key_name, value_name],
                                 order_by=key_name, **kwargs))
        for row in result:
            yield row[value_name]

    def drop(self, table):
        logger.info('dropping table: %s' % table)
        drop_table(self.db, table)
        assert table not in self.tables
        logger.info('table: %s dropped' % table)

    def create_and_load_table(self, stream, structure):
        table = get_table(self.db, structure)
        table.load_table(stream)
