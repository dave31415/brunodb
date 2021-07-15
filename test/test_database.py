from tempfile import NamedTemporaryFile
from brunodb.sqlite_utils import get_db, SQLiteDB
from brunodb.database_sqlite import db_is_open, DBaseSqlite


def test_db_is_open():
    db = get_db(filename=None, isolation_level="DEFERRED", journal_mode='OFF')
    assert isinstance(db, SQLiteDB)
    assert db_is_open(db)
    db.close()
    assert not db_is_open(db)


def test_dbase():
    dbase = DBaseSqlite(None)
    assert isinstance(dbase, DBaseSqlite)
    assert dbase.db_file is None
    dbase.close()
    assert not dbase.is_open()


def test_dbase_file():
    filename = NamedTemporaryFile().name
    dbase = DBaseSqlite(filename)
    assert isinstance(dbase, DBaseSqlite)
    assert dbase.db_file == filename
    dbase.close()
    assert not dbase.is_open()
