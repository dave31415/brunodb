from tempfile import NamedTemporaryFile
import pytest
from brunodb.sqlite_utils import get_db, SQLiteDB
from brunodb.database_sqlite import db_is_open, DBaseSqlite
from brunodb.database import DBase


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


def test_dbase_wrapper_sqlite():
    config = {'db_type': 'sqlite'}
    dbase = DBase(config)
    assert isinstance(dbase, DBaseSqlite)
    assert dbase.db.db_type == 'sqlite'
    assert dbase.db_file is None
    assert dbase.is_open()
    dbase.close()


def test_dbase_wrapper_sqlite_with_file():
    filename = NamedTemporaryFile().name
    config = {'db_type': 'sqlite', 'filename': filename,
              'isolation_level': "IMMEDIATE", 'journal_mode': 'WAL'}
    dbase = DBase(config)
    assert dbase.db.db_type == 'sqlite'
    assert dbase.db_file == filename
    assert dbase.is_open()
    mode = list(dbase.db.execute('pragma journal_mode'))[0]
    assert mode == {'journal_mode': 'wal'}

    dbase.close()


def test_dbase_wrapper_sqlite_with_no_db_type():
    config = {}
    with pytest.raises(KeyError):
        DBase(config)
