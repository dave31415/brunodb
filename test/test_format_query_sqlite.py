from brunodb.run_cars_example import run_cars
from brunodb.format_query import format_sql_in_context


def test_format_in_context_postgres_with_template_and_values():
    dbase = run_cars('sqlite', no_close=True)

    params = {'field': 'name', 'table': 'cars'}

    template = 'select {field} from {table} where cylinders = ?'
    sql = format_sql_in_context(template, params, None)
    cars = dbase.raw_sql_query(sql, values=(6,))
    car_names = [car['name'] for car in cars]
    assert len(car_names) == 7

    expected = ['Mazda RX4', 'Mazda RX4 Wag', 'Hornet 4 Drive',
                'Valiant', 'Merc 280', 'Merc 280C', 'Ferrari Dino']

    assert set(car_names) == set(expected)

    dbase.drop('cars')
    dbase.close()
