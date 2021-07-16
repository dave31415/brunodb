from brunodb.database import DBase
from brunodb.bulk_load_postgres import bulk_load_cars, get_cars_file_name
from brunodb.cars_example import get_cars_structure


def test_bulk_load():
    config = {'db_type': 'postgres'}
    dbase = DBase(config)
    dbase.drop('cars')
    assert 'cars' not in dbase.tables
    bulk_load_cars(dbase)
    assert 'cars' in dbase.tables
    cars_list = list(dbase.query('cars'))
    assert len(cars_list) == 32


def test_bulk_load_from_file():
    config = {'db_type': 'postgres'}
    dbase = DBase(config)
    dbase.drop('cars')
    assert 'cars' not in dbase.tables
    filename = get_cars_file_name()
    structure = get_cars_structure()
    dbase.create_and_load_table_from_csv(filename, structure, bulk_load=True)
    assert 'cars' in dbase.tables
    cars_list = list(dbase.query('cars'))
    assert len(cars_list) == 32
