from brunodb.run_cars_example import run_cars


def test_cars_postgres_block():
    run_cars('postgres', block=True)


def test_cars_postgres_non_block():
    run_cars('postgres', block=False)


def test_cars_drop():
    dbase = run_cars('postgres', no_close=True)
    assert dbase.tables == ['cars']
    cars_list = list(dbase.query('cars'))
    n_cars = len(cars_list)
    assert n_cars == 32
    dbase.drop('cars')
    assert dbase.tables == []
