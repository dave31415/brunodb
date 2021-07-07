from sqlite3 import Connection
from tempfile import NamedTemporaryFile
from brunodb.sqlite_utils import get_db
from brunodb.database import db_is_open, DBase


def test_db_is_open():
    db = get_db(filename=None, isolation_level="DEFERRED", journal_mode='OFF')
    assert isinstance(db, Connection)
    assert db_is_open(db)
    db.close()
    assert not db_is_open(db)


def test_dbase():
    dbase = DBase(None)
    assert isinstance(dbase, DBase)
    assert dbase.db_file is None
    dbase.close()
    assert not dbase.is_open()


def test_dbase_file():
    filename = NamedTemporaryFile().name
    dbase = DBase(filename)
    assert isinstance(dbase, DBase)
    assert dbase.db_file == filename
    dbase.close()
    assert not dbase.is_open()
