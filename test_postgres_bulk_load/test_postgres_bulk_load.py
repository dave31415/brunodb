from brunodb.database import DBase
from brunodb.bulk_load_postgres import bulk_load_cars


def test_bulk_load():
    config = {'db_type': 'postgres'}
    dbase = DBase(config)
    dbase.drop('cars')
    assert 'cars' not in dbase.tables
    bulk_load_cars(dbase)
    assert 'cars' in dbase.tables
    cars_list = list(dbase.query('cars'))
    assert len(cars_list) == 32
