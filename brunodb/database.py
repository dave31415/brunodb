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


class DBase:
    def __init__(self, db_file, isolation_level="DEFERRED", journal_mode="OFF"):
        self.db_file = db_file
        self.db = get_db(filename=db_file,
                         isolation_level=isolation_level,
                         journal_mode=journal_mode)

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
        table.load_table(stream, block=True)
