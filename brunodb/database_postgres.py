import logging
import psycopg2
from brunodb.postgres_utils import PostgresDB
from brunodb.database_generic import DBaseGeneric
logger = logging.getLogger(__file__)
database_global = {}


def db_is_open(connection):
    try:
        cur = connection.cursor()
        cur.execute('SELECT 1')
    except psycopg2.OperationalError:
        pass


class DBasePostgres(DBaseGeneric):
    def __init__(self, config=None):
        super().__init__()
        self.config = config
        self.db = PostgresDB(config=config)
        self.place_holder = "%s"

        logger.info('Tables: %s' % self.tables.__repr__())

    def is_open(self):
        return db_is_open(self.db.con)
