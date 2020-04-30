from csv import DictReader
import os


def get_cars_structure():
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
    filename = "%s/cars.csv" % get_example_data_dir()
    print('Reading cars data from: %s' % filename)
    return (dict(row) for row in DictReader(open(filename, 'r')))


def load_cars_table(dbase):
    stream = stream_cars()
    structure = get_cars_structure()
    dbase.create_and_load_table(stream, structure)
