from csv import DictReader
import os
from math import sqrt
from itertools import cycle, groupby
from brunodb.database import DBase
from brunodb.format_query import format_sql_in_context

# code for building cars table from included cars.csv file


def get_cars_structure():
    # Every table needs a schema like this, good idea to use portable
    # data types that work for both SQLite and Postgres
    return {'table_name': 'cars',
            'schema': {'name': 'TEXT NOT NULL',
                       'mpg': 'REAL',
                       'cylinders': 'REAL',
                       'displacement': 'REAL',
                       'horsepower': 'REAL'},
            'indices': ['name', 'mpg']}


def get_example_data_dir():
    return os.path.realpath(os.path.dirname(__file__)+'/../example_data')


def stream_cars():
    # Get the venerable old cars dataset often used in demos, 32 in all
    filename = "%s/cars.csv" % get_example_data_dir()
    return (dict(row) for row in DictReader(open(filename, 'r')))


def stream_cars_repeat(n_iters):
    # get more car records if I want by cycling through many times
    stream = cycle(stream_cars())
    for i in range(n_iters):
        yield next(stream)


def load_cars_table(dbase, block=False):
    # typical load statement where you fetch the stream through a function call
    # create the structure and load into the database
    stream = stream_cars()
    structure = get_cars_structure()
    dbase.create_and_load_table(stream, structure, block=block)

###################################
# Now make use of this with a demo


def print_list(a_list):
    # just prints a list, one per line
    for row in a_list:
        print(row)


def demo():
    # create in memory sqlite db, add 'filename' to config if you want to persist in a file
    # if you have postgres install can do config = {'db_type': 'postgres', ...}
    # with postgres config params

    config = {'db_type': 'sqlite'}
    dbase = DBase(config)
    print('Should have no tables')
    print(dbase.tables)

    load_cars_table(dbase)
    print('Should have cars table')
    print(dbase.tables)

    print('Should have 32 cars')
    print("%s rows in cars" % dbase.c('cars'))

    # query the table, 'q' is an alias for longer 'query' methods
    stream = dbase.q('cars')

    print(next(stream))

    all_cars = list(dbase.q('cars'))
    print(len(all_cars))
    print_list(all_cars)

    # do a raw SQL query, r is an an alias for longer raw_sql_query
    my_car = list(dbase.r("select * from cars where name = 'Ferrari Dino'"))[0]
    print('\nMy car is:')
    print(my_car)
    print(my_car['horsepower'])

    # safe formatting (safe for Postgres, relatively safe for SQLite, see docs)
    # placeholders will hold data-types (numbers, strings etc) and that safe
    # formatting is done by sqlite3 and psycopg2 client libraries
    # identifiers like table names, column names etc are different.
    # Identifiers are handled safely by client library in Postgres but
    # sqlite3 doesn't provide this so we wrote our own simple version which
    # is safer than nothing but not guaranteed secure (allows only certain characters).
    # So be careful for SQLite in production with user given inputs.
    # In Postgres context would be dbase.db.con instead of None

    table_name = 'cars'
    field = 'horsepower'
    my_car_name = 'Ferrari Dino'
    values = [my_car_name]

    query_template = "select {field} from {table_name} where name = %s" % dbase.place_holder
    params = {'table_name': table_name, 'field': field}
    context_sqlite = None
    query = format_sql_in_context(query_template, params, context_sqlite)

    # values (a list) are includes as a second parameter to fill in the
    # placeholders, works with query as well as raw_sql_query

    result = list(dbase.r(query, values))[0]
    print_list(result)

    # query as has some shortcuts to avoid having to write full SQL queries
    print('\nSelect by a field\n---------')
    print_list(list(dbase.q('cars', name='Ferrari Dino')))

    print('\nJust name and horsepower\n---------')
    print_list(list(dbase.q('cars', fields=['name', 'horsepower'])))

    print('\nAdd a where filter\n---------')
    print_list(list(dbase.q('cars', fields=['cylinders', 'name', 'horsepower'],
                            where_extra='cylinders = 8')))

    # must flush this stream before dropping table
    # to avoid: OperationalError: database table is locked
    # In general SQLite should not be used for concurrent programming
    # and might give hard to debug errors concerning database locking
    # Use Postgres for concurrency

    _ = list(stream)

    # can truncate and drop tables
    dbase.truncate('cars')
    print('Tables')
    print(dbase.tables)

    print('N rows')
    print(dbase.c('cars'))
    dbase.drop('cars')

    print('Should have no tables now')
    print(dbase.tables)

    dbase.close()

    # A useful pattern is to use the database to order records and then use the python
    # itertools function, groupby. This gives you a lot more control over aggregations because
    # you can write any aggregation functions in python. If you are just doing SUM, MIN, MAX etc
    # you might want to just use SQL GROUP BY as a query but for more complex computations
    # you are better off writing modular, unit-testable functions in python.
    # This gives you the best parts of using databases without the worst part
    # (code clarity and testability).

    dbase = DBase(config)
    load_cars_table(dbase)

    def agg_by_cylinder_func(cylinders, rows):
        # a non-trivial, unit-testable, aggregation function
        horse_power = [i['horsepower'] for i in rows]

        max_horsepower = max(horse_power)
        min_horsepower = max(min(horse_power), 100)

        if cylinders > 4:
            max_horsepower_adjusted = max_horsepower * sqrt(4./cylinders)
        else:
            max_horsepower_adjusted = max_horsepower * 0.972

        return {'cylinders': cylinders,
                'min_horsepower': min_horsepower,
                'max_horsepower': max_horsepower,
                'max_horsepower_adjusted': max_horsepower_adjusted}

    stream = dbase.q('cars', order_by='cylinders')
    aggs = [agg_by_cylinder_func(cylinders, group_rows)
            for cylinders, group_rows in groupby(stream, lambda x: x['cylinders'])]

    print('Aggregate statistics')
    print_list(aggs)

    # One little hack (might be improved in the future to an actual command)
    # For stopping a long running job that is writing to the database
    # you can create a file that is given by environment variable BRUNODB_STOP_FILE
    # The database checks for that file occasionally and will stop if it sees it
    # after closing the database and exiting more gracefully.
    # This can avoid corrupting the database (especially for SQLite) from a
    # suddenly killed process. Defaults to BRUNODB_STOP_FILE in the brunodb root dir
    # Be sure to remove it afterwards
