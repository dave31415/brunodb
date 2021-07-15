import pytest
from brunodb.format_query import format_query_check_chars, format_sql_in_context
from brunodb.postgres_utils import open_connection
from brunodb.run_cars_example import run_cars


def test_format_check_chars():
    template = "select {field} from {table}"

    params = {'field': 'name', 'table': 'customers'}
    result = format_query_check_chars(template, params)
    expected = "select name from customers"
    assert result == expected

    with pytest.raises(ValueError):
        # raises an error
        params = {'field': "-*'", 'table': "'-"}
        result = format_query_check_chars(template, params)
        expected = "select -*' from '-"
        assert result == expected


def test_format_in_context_sqlite():
    template = "select {field} from {table}"

    params = {'field': 'name', 'table': 'customers'}
    result = format_sql_in_context(template, params, None)
    expected = "select name from customers"
    assert result == expected

    with pytest.raises(ValueError):
        # raises an error
        params = {'field': "-*'", 'table': "'-"}
        result = format_sql_in_context(template, params, None)
        expected = "select -*' from '-"
        assert result == expected


def test_format_in_context_postgres():
    conn = open_connection()

    template = "select {field} from {table}"

    params = {'field': 'name', 'table': 'customers'}
    result = format_sql_in_context(template, params, conn)
    expected = 'select "name" from "customers"'
    assert result == expected


def test_format_in_context_postgres_with_template():
    dbase = run_cars('postgres', no_close=True)

    template = "select {field} from {table}"
    params = {'field': 'name', 'table': 'cars'}

    template = 'select {field} from {table} where cylinders = 6.0'
    sql = format_sql_in_context(template, params, dbase.db.con)
    cars = dbase.raw_sql_query(sql)
    car_names = [car['name'] for car in cars]
    assert len(car_names) == 7

    expected = ['Mazda RX4', 'Mazda RX4 Wag', 'Hornet 4 Drive',
                'Valiant', 'Merc 280', 'Merc 280C', 'Ferrari Dino']

    assert set(car_names) == set(expected)

    dbase.drop('cars')
    dbase.close()


def test_format_in_context_postgres_with_template_and_values():
    dbase = run_cars('postgres', no_close=True)

    params = {'field': 'name', 'table': 'cars'}

    template = 'select {field} from {table} where cylinders = %s'
    sql = format_sql_in_context(template, params, dbase.db.con)
    cars = dbase.raw_sql_query(sql, values=(6,))
    car_names = [car['name'] for car in cars]
    assert len(car_names) == 7

    expected = ['Mazda RX4', 'Mazda RX4 Wag', 'Hornet 4 Drive',
                'Valiant', 'Merc 280', 'Merc 280C', 'Ferrari Dino']

    assert set(car_names) == set(expected)

    dbase.drop('cars')
    dbase.close()
