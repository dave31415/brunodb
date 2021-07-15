from brunodb.database import DBase
from brunodb.database_postgres import DBasePostgres


def test_dbase_wrapper_sqlite_with_file():
    config = {'db_type': 'postgres'}
    dbase = DBase(config)
    assert isinstance(dbase, DBasePostgres)
    assert dbase.db.db_type == 'postgres'
    assert dbase.is_open()
    dbase.close()
