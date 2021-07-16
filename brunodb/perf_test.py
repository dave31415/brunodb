from tempfile import NamedTemporaryFile
from time import time
from csv import DictReader, DictWriter
from brunodb.cars_example import stream_cars_repeat, get_cars_structure
from brunodb import DBase


bytes_per_line = 31.3
mega_byte = 1048576


# isolation_levels = [None, "DEFERRED", "IMMEDIATE", "EXCLUSIVE"]
# it is slow with None

isolation_levels = ["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]
journal_modes = ['DELETE', 'TRUNCATE', 'PERSIST', 'MEMORY', 'WAL', 'OFF']


def print_timing(label, start, n_rows):
    runtime = time() - start
    rate = n_rows/runtime
    rate_mbytes_per_sec = rate * bytes_per_line/mega_byte

    label = label.ljust(40)
    runtime = ("%0.3f" % runtime).rjust(4)
    rate_string = '{:,}'.format(int(rate)).rjust(10)
    rate_mbytes_per_sec = ("%0.3f" % rate_mbytes_per_sec).rjust(6)

    info = (label, n_rows, runtime, rate_string, rate_mbytes_per_sec)
    print("%s: runtime: n_rows: %s, %s seconds, rate: %s rows/sec, rate: %s MB/sec" % info)
    return rate


def load_test(num=10000, memory=False, isolation_level='DEFERRED', journal_mode='OFF',
              read_test=False, db_type='sqlite', block=False, no_indices=False, bulk_load=False):
    stream = stream_cars_repeat(num)

    if db_type == 'sqlite':
        if memory:
            filename = None
        else:
            filename = NamedTemporaryFile().name

        config = {'db_type': db_type,
                  'filename': filename,
                  'isolation_level': isolation_level,
                  'journal_mode': journal_mode}

        dbase = DBase(config)
    elif db_type == 'postgres':
        config = {'db_type': db_type}
        dbase = DBase(config)
    else:
        raise ValueError('Unknown db_type: %s' % db_type)

    dbase.drop('cars')
    structure = get_cars_structure()
    if no_indices:
        structure['indices'] = []

    if read_test:
        dbase.create_and_load_table(stream, structure, block=block)

        start = time()
        _ = list(dbase.query('cars'))
        label = 'Read test'
        print_timing(label, start, num)
    else:
        start = time()
        dbase.create_and_load_table(stream, structure, bulk_load=bulk_load)

        print('------------------------------------')
        label = '%s Block: %s, Mem: %s, Iso: %s, JM: %s' % (db_type, block, memory, isolation_level, journal_mode)
        print_timing(label, start, num)
        print(config)


def file_test(num=10000):
    start = time()
    filename = NamedTemporaryFile().name
    stream = stream_cars_repeat(num)
    row = next(stream)
    fieldnames = list(row.keys())
    fp = open(filename, 'w')
    wr = DictWriter(fp, fieldnames=fieldnames)
    wr.writeheader()
    wr.writerow(row)
    wr.writerows(stream)
    fp.close()
    print_timing('FILE IO WRITE', start, num)

    fp = open(filename, 'r')
    wr = DictReader(fp)
    for _ in wr:
        pass

    fp.close()
    print_timing('FILE IO READ', start, num)


def load_test_one(num=100000, read_test=False, **kwargs):
    load_test(num=num, read_test=read_test, **kwargs)


def load_test_all(num=100000):
    file_test(num)
    file_test(num)
    print("Write tests")
    print('--------------------------')
    load_test(num=num)
    load_test(num=num, memory=True, isolation_level='DEFERRED', journal_mode='OFF')
    for iso in isolation_levels:
        for journal in journal_modes:
            load_test(num=num, memory=False, isolation_level=iso, journal_mode=journal)

    print("Read tests")
    print('--------------------------')
    load_test(num=num, read_test=True)
