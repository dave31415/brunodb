# brunodb
Lightweight but useful python interface for sqlite and postgres

There are no real dependencies besides sqlite3 which is a standard library 
module and pytest for running tests. psycopg2 and a postgres database is needed
to run the interface on postgres.

To install

pip install brunodb

See these for examples of how to use

https://github.com/dave31415/brunodb/blob/master/brunodb/cars_example.py


https://github.com/dave31415/brunodb/blob/master/test/test_cars.py

To run tests:

python -m pytest test

If you have postgres installed, you can test it as well. You'll need to put
the database password in the POSTGRES_PWD environment variable and have the
usual standards: running on localhost, usual port, user name postgres etc.

```
python -m pytest test_postscript 
```

Or run all tests

```
python -m pytest
```

There is a wrapper for either Database class called DBase:

For in memory sqlite database:

```
from brunodb import DBase
config = {'db_type': 'sqlite'}
dbase = DBase(config)
```

Or with a file:

```
config = {'db_type': 'sqlite', 'filename': 'path/my_database.db'}
dbase = DBase(config)
```

Or using postgres:
```
config = {'db_type': 'postgres'}
dbase = DBase(config)
```

Or:
```
config = {'db_type': 'postgres', 'port': 5555, 'password':'123Bannana'}
dbase = DBase(config)
```
