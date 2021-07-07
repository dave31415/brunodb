from sqlite3 import Connection
from brunodb.sqlite_utils import get_db
from brunodb.database import db_is_open


def test_db_is_open():
    db = get_db(filename=None, isolation_level="DEFERRED", journal_mode='OFF')
    assert isinstance(db, Connection)
    assert db_is_open(db)
    db.close()
    assert not db_is_open(db)
