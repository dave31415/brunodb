import logging
from brunodb.sqlite_utils import drop_table
from brunodb.query import get_query_sql
from brunodb.table import get_table

logger = logging.getLogger(__file__)


class DBaseGeneric:
    def __init__(self):
        self.place_holder = "?"
        self.db = None

    def query(self, table, count_table_rows=False, **kwargs):
        sql, vals = get_query_sql(table, count_table_rows=count_table_rows, place_holder=self.place_holder,
                                  **kwargs)
        show_sql = False
        if show_sql:
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
        return self.db.get_tables()

    def drop(self, table):
        logger.info('dropping table: %s' % table)
        drop_table(self.db, table)
        assert table not in self.tables
        logger.info('table: %s dropped' % table)

    def create_and_load_table(self, stream, structure, block=False):
        table = get_table(self.db, structure)
        table.load_table(stream, block=block)

    def close(self):
        self.db.close()

    def is_open(self):
       pass
