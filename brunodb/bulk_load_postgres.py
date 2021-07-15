from tempfile import NamedTemporaryFile
from csv import DictWriter
import os
import subprocess
from brunodb import DBase
from brunodb.cars_example import get_cars_structure


def dump_stream(stream, filename=None):
    if filename is None:
        filename = NamedTemporaryFile().name

    print('Writing data to file: %s' % filename)
    fp = open(filename, 'w')
    first = next(stream)
    fieldnames = first.keys()
    writer = DictWriter(fp, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow(first)
    writer.writerows(stream)
    fp.close()
    print('Finish wriite file')
    return filename


def get_connection_string(table_name, password=None):
    if password is None:
        password = os.getenv('POSTGRES_PWD')
    assert password is not None

    connection_string = "postgres://postgres:{password}@127.0.0.1:5432/postgres#{table_name}"
    return connection_string.format(table_name=table_name,
                                    password=password)


def bulk_load_file(filename, table_name, password=None):
    connection_string = get_connection_string(table_name, password=password)

    commands = ['dbcrossbar', 'cp', '--if-exists=overwrite',
                'csv:%s' % filename, connection_string]
    # print(' '.join(commands))

    subprocess.run(commands)


def bulk_load_stream(stream, table_name, filename=None, password=None):
    filename = dump_stream(stream, filename=filename)
    bulk_load_file(filename, table_name, password=password)


def bulk_load_cars():
    path = os.path.dirname(__file__) + '/..'
    path = os.path.realpath(path)
    filename = '%s/example_data/cars.csv' % path
    assert os.path.exists(filename)
    config = {'db_type': 'postgres'}
    dbase = DBase(config)

    if 'cars' in dbase.tables:
        dbase.truncate('cars')
    else:
        structure = get_cars_structure()
        dbase.create_table(structure)
        dbase.close()

    bulk_load_file(filename, 'cars')
