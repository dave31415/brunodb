import os
import sys
from sqlite3 import Connection


STOP_FILE = os.getenv('BRUNODB_STOP_FILE', 'BRUNODB_STOP_FILE')


def stop_gracefully(db, no_exit=False):
    """
    A mechanism to stop the python process that is reading/writing to brunodb and
    shutdown gracefully, i.e. close the database first. Better than a hard stop
    which might corrupt the database.
    :param db: the database object
    :param no_exit: Default False, if True, won't exit Python and
        instead will just return the stop Boolean so the program can handle the
        exit.
    :return: Boolean, whether to stop (if no_exit is True)
    """

    stop = os.path.exists(STOP_FILE)

    if not stop:
        return

    db.close()

    message = "Brunodb stop file, %s exists so database will be closed" % STOP_FILE
    if no_exit:
        print(message)
        return True

    message += ' and python will exit'
    print(message)
    sys.exit()


def graceful_exit(func):
    """
    To be used as a decorator. Will find the database variable and close it.
    There had better be one and just one or error will be raised
    :param func: function to be wrapped
    :return: wrapped function
    """

    def wrapper(*args, **kwargs):
        db_args = [arg for arg in args if isinstance(arg, Connection)]
        kw_db_args = [val for val in kwargs.values() if isinstance(val, Connection)]
        db = db_args + kw_db_args
        if len(db) == 0:
            raise ValueError('No database variable to close database')
        if len(db) > 1:
            raise ValueError('Ambiguous database variable to close database')

        db = db[0]

        stop_gracefully(db)

        func(*args, **kwargs)

    return wrapper