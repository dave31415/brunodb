from tempfile import NamedTemporaryFile
from brunodb import DBase
from brunodb.cars_example import load_cars_table


def test_cars():
    filename = NamedTemporaryFile().name
    print('Database filename: %s' % filename)
    dbase = DBase(filename)
    load_cars_table(dbase)
    assert 'cars' in dbase.tables
    cars = list(dbase.query('cars'))
    n_cars = len(cars)
    assert n_cars == 32

    cars = list(dbase.query('cars', cylinders=6.0))
    n_cars = len(cars)
    assert n_cars == 7

    sql = 'select name from cars where cylinders = 6.0'
    cars = dbase.raw_sql_query(sql)
    car_names = [car['name'] for car in cars]
    assert len(car_names) == 7
    expected = ['Mazda RX4', 'Mazda RX4 Wag', 'Hornet 4 Drive',
                'Valiant', 'Merc 280', 'Merc 280C', 'Ferrari Dino']

    assert car_names == expected

    sql = 'select count(*) as number from cars where cylinders = 6.0'
    result = list(dbase.raw_sql_query(sql))[0]['number']
    assert result == 7
