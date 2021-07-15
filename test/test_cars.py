from brunodb.run_cars_example import run_cars


def test_cars_block():
    run_cars('sqlite', block=True)


def test_cars_non_block():
    run_cars('sqlite', block=False)


def test_cars_drop():
    dbase = run_cars('sqlite', no_close=True)
    assert dbase.tables == ['cars']
    cars_list = list(dbase.query('cars'))
    n_cars = len(cars_list)
    assert n_cars == 32
    dbase.drop('cars')
    assert dbase.tables == []
