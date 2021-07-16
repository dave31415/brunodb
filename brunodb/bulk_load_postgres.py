from tempfile import NamedTemporaryFile
from csv import DictWriter
import os
import subprocess
from brunodb.cars_example import get_cars_structure
from brunodb.table import get_table
from time import time

# Must install dbcrossbar bulk loader first
# http://www.dbcrossbar.org


def dump_stream(stream, filename=None):
    if filename is None:
        filename = NamedTemporaryFile().name+'.csv'

    print('Writing data to file: %s' % filename)
    fp = open(filename, 'w')
    first = next(stream)
    fieldnames = first.keys()
    writer = DictWriter(fp, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow(first)
    writer.writerows(stream)
    fp.close()
    print('Finish writing file')
    return filename


def get_connection_string(table_name, password=None):
    if password is None:
        password = os.getenv('POSTGRES_PWD')
    assert password is not None

    connection_string = "postgres://postgres:{password}@127.0.0.1:5432/postgres#{table_name}"
    return connection_string.format(table_name=table_name,
                                    password=password)


def run_command(commands):
    try:
        _ = subprocess.check_output(commands,
                                    stderr=subprocess.STDOUT,
                                    universal_newlines=True)

    except subprocess.CalledProcessError as exc:
        print("Failed", exc.returncode, exc.output)
        raise exc


def bulk_load_file(db, filename, structure, password=None):
    _ = get_table(db, structure)
    connection_string = get_connection_string(structure['table_name'], password=password)
    commands = ['dbcrossbar', 'cp', '--if-exists=overwrite', 'csv:%s' % filename, connection_string]
    # subprocess.run(commands)
    run_command(commands)


def bulk_load_stream(db, stream, structure, filename=None, password=None):
    start = time()
    filename = dump_stream(stream, filename=filename)
    dump_time = time() - start
    bulk_load_file(db, filename, structure, password=password)
    run_time = time() - start
    print('run_time: %0.5f seconds' % run_time)
    print('%0.5f seconds in file dump' % dump_time)


def get_cars_file_name():
    path = os.path.dirname(__file__) + '/..'
    path = os.path.realpath(path)
    return '%s/example_data/cars.csv' % path


def bulk_load_cars(dbase):
    filename = get_cars_file_name()
    structure = get_cars_structure()
    bulk_load_file(dbase.db, filename, structure)
