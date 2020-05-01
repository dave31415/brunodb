from tempfile import NamedTemporaryFile
from time import time
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
    rate = '{:,}'.format(int(rate)).rjust(10)
    rate_mbytes_per_sec = ("%0.3f" % rate_mbytes_per_sec).rjust(6)

    info = (label, n_rows, runtime, rate, rate_mbytes_per_sec)
    print("%s: runtime: n_rows: %s, %s seconds, rate: %s rows/sec, rate: %s MB/sec" % info)


def load_test(num=10000, memory=False, isolation_level='DEFERRED', journal_mode='OFF', read_test=False):
    stream = stream_cars_repeat(num)
    filename = NamedTemporaryFile().name
    if memory:
        filename = None

    dbase = DBase(filename,
                  isolation_level=isolation_level,
                  journal_mode=journal_mode)

    dbase.drop('cars')
    structure = get_cars_structure()

    if read_test:
        dbase.create_and_load_table(stream, structure)

        start = time()
        _ = list(dbase.query('cars'))
        label = 'Read test'
        print_timing(label, start, num)
    else:
        start = time()
        dbase.create_and_load_table(stream, structure)

        label = 'Mem: %s, Iso: %s, JM: %s' % (memory, isolation_level, journal_mode)
        print_timing(label, start, num)


def load_test_all(num=1000000):
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
