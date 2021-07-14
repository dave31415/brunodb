from brunodb.database_postgres import DBasePostgres
from brunodb.cars_example import load_cars_table


def run_cars_postgres(block):
    dbase = DBasePostgres()

    # load the cars table, see cars_example file for how this is done
    load_cars_table(dbase, block=block)

    # Check that there is now a table called cars

    assert dbase.tables == ['cars']

    # Use use the simple query API and read all records
    # equivalent to "select * from cars"
    # returns a list of dicts

    cars = list(dbase.query('cars'))
    n_cars = len(cars)
    assert n_cars == 32

    # use the simple API to select only 6 cylinder cars
    # should be fewer now
    cars = list(dbase.query('cars', cylinders=6.0))
    n_cars = len(cars)
    assert n_cars == 7

    # use the raw sql API to run the same query, keeping only the
    # 'name' field
    sql = 'select name from cars where cylinders = 6.0'
    cars = dbase.raw_sql_query(sql)
    car_names = [car['name'] for car in cars]
    assert len(car_names) == 7

    expected = ['Mazda RX4', 'Mazda RX4 Wag', 'Hornet 4 Drive',
                'Valiant', 'Merc 280', 'Merc 280C', 'Ferrari Dino']

    assert car_names == expected

    # run another SQL query
    sql = 'select count(*) as number from cars where cylinders = 6.0'
    result = list(dbase.raw_sql_query(sql))[0]['number']
    assert result == 7


def test_cars_postgres_block():
    run_cars_postgres(True)


def test_cars_postgres_non_block():
    run_cars_postgres(False)
