import sqlite3
import logging
from brunodb.sqlite_utils import get_db
from brunodb.database_generic import DBaseGeneric

logger = logging.getLogger(__file__)


def db_is_open(db):
    try:
        db.execute('SELECT 1')
    except sqlite3.ProgrammingError:
        return False

    return True


class DBaseSqlite(DBaseGeneric):
    def __init__(self, db_file, isolation_level=None, journal_mode=None):
        if isolation_level is None:
            isolation_level = "DEFERRED"
        if journal_mode is None:
            journal_mode = "OFF"

        super().__init__()
        self.db_file = db_file
        self.db = get_db(filename=db_file,
                         isolation_level=isolation_level,
                         journal_mode=journal_mode)
        self.db_type = 'sqlite'

        logger.info('Tables: %s' % self.tables.__repr__())

    def is_open(self):
        return db_is_open(self.db)
