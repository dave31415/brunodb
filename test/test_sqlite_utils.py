from tempfile import NamedTemporaryFile
import os
from brunodb.sqlite_utils import get_db, create_lookup_table, list_tables, drop_table, \
    schema_to_schema_string, get_tables, SQLiteDB


def test_get_db():
    db = get_db(filename=None, isolation_level="DEFERRED", journal_mode='OFF')
    assert isinstance(db, SQLiteDB)
    db.close()

    filename = NamedTemporaryFile().name
    db = get_db(filename=filename, isolation_level="DEFERRED", journal_mode='OFF')
    assert isinstance(db, SQLiteDB)
    assert os.path.exists(filename)
    mode = list(db.execute('pragma journal_mode'))[0]
    assert mode == {'journal_mode': 'off'}
    db.close()


def test_get_db_journal_modes():
    journal_modes = ['DELETE', 'TRUNCATE', 'PERSIST', 'MEMORY', 'WAL', 'OFF']
    for journal_mode in journal_modes:
        filename = NamedTemporaryFile().name
        db = get_db(filename=filename, isolation_level="DEFERRED", journal_mode=journal_mode)
        assert isinstance(db, SQLiteDB)
        assert os.path.exists(filename)
        mode = list(db.execute('pragma journal_mode'))[0]
        assert mode == {'journal_mode': journal_mode.lower()}
        db.close()

    # TODO: test for isolation levels


def test_create_lookup_table():
    db = get_db(filename=None)
    assert list_tables(db) == []
    table_name = 'foo'
    create_lookup_table(db, table_name, key_type='TEXT', value_type='TEXT',
                        drop_first=False)
    assert list_tables(db) == [{'name': 'foo'}]

    drop_table(db, table_name)
    assert list_tables(db) == []

    db.close()


def test_schema_to_schema_string():
    schema = {'name': 'TEXT NOT NULL',
              'mpg': 'REAL',
              'cylinders': 'REAL',
              'displacement': 'REAL',
              'horsepower': 'REAL'}

    string = schema_to_schema_string(schema)
    expected = "name TEXT NOT NULL, mpg REAL, cylinders REAL, displacement REAL, horsepower REAL"
    assert string == expected


def test_get_tables():
    db = get_db(filename=None)
    assert list_tables(db) == []
    table_name = 'foo'
    create_lookup_table(db, table_name, key_type='TEXT', value_type='TEXT',
                        drop_first=False)
    tables = get_tables(db)
    assert tables == ['foo']
