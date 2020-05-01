import logging
from time import time
from brunodb.sqlite_utils import drop_table, drop_index, schema_to_schema_string
from brunodb.sqlite_utils import get_tables

logger = logging.getLogger(__name__)


class Table(object):
    def __init__(self, db, table_name, schema, index_fields):
        self.db = db
        self.table = table_name
        self.index_fields = index_fields
        self.schema = schema
        self.fields = list(self.schema.keys())
        self.n_fields = len(self.schema)

    def create_table(self):
        logger.info('Creating table (and indices): %s' % self.table)
        drop_table(self.db, self.table)

        schema_string = schema_to_schema_string(self.schema)

        sql = "CREATE TABLE {table} ( {schema_string} )".format(table=self.table,
                                                                schema_string=schema_string)
        with self.db:
            self.db.execute(sql)

        for index_field in self.index_fields:
            with self.db:
                self.create_index(index_field)

        # Just to safe, probably not needed
        self.db.commit()

    def create_index(self, index_field):
        with self.db:
            index_name = "index_{table}_{index_field}".format(table=self.table,
                                                              index_field=index_field)
            drop_index(self.db, index_name)
            sql_template = "CREATE INDEX {index_name} ON {table} ({index_field})"
            sql = sql_template.format(table=self.table,
                                      index_name=index_name,
                                      index_field=index_field)
            self.db.execute(sql)

    def _insert_many(self, values_list):
        questions = ','.join(['?' for _ in range(self.n_fields)])
        format_vals = '(' + questions + ')'
        sql = "INSERT INTO {table} VALUES {format_vals}".format(table=self.table,
                                                                format_vals=format_vals)
        with self.db:
            self.db.executemany(sql, values_list)

    def _insert_many_non_block(self, values_list):
        # If there are multiple processes writing from streams
        # don't create transaction around entire stream
        # Do one at a time. But slower.
        start = time()

        questions = ','.join(['?' for _ in range(self.n_fields)])
        format_vals = '(' + questions + ')'
        sql = "INSERT INTO {table} VALUES {format_vals}".format(table=self.table,
                                                                format_vals=format_vals)
        log_every = 1000
        commit_every = 1000
        last_time = time()
        for row_num, values in enumerate(values_list):
            if row_num % log_every == 0 and row_num > 0:
                this_time = time()

                runtime_segment = this_time - last_time
                rate_segment = log_every / runtime_segment

                runtime = this_time - start
                rate = row_num/runtime
                vals = (row_num, self.table, rate_segment, rate)
                message = "Writing row: %s for table: %s, rate_segment: %0.4f rows/sec, rate_all: %0.4f rows/sec"
                logger.info(message % vals)
                last_time = this_time

            self.db.execute(sql, values)

            if row_num % commit_every == 0:
                self.db.commit()

        self.db.commit()

    def _get_values_from_row(self, row):
        return [row[k] for k in self.fields]

    def _stream_values(self, stream, max_rows):
        for row_num, row in enumerate(stream):
            if row_num == max_rows:
                return

            values_list = self._get_values_from_row(row)
            yield values_list

    def load_table(self, stream, max_rows=1000000000000,
                   create=True, block=True):

        if create or self.table not in get_tables(self.db):
            self.create_table()

        values_list = self._stream_values(stream, max_rows)

        if block:
            self._insert_many(values_list)
        else:
            self._insert_many_non_block(values_list)

        # Just to be sure
        self.db.commit()

    def lookup(self, key):
        sql_template = "SELECT * FROM {table} WHERE {index_field} = '{key}'"
        sql = sql_template.format(table=self.table, index_field=self.index_fields[0], key=key)
        with self.db:
            result = self.db.execute(sql).fetchall()

        if result is None:
            return None

        return [{k: v for k, v in zip(self.fields, res)} for res in result]


def get_table(db, structure):
    return Table(db,
                 structure['table_name'],
                 structure['schema'],
                 structure.get('indices', []))
