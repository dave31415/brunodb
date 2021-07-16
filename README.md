# brunodb
Brunodb is a lightweight but useful python interface for sqlite and 
postgres. It is tailored to data science workflows which are basically 
high throughout streaming computation patterns (rather than 
transactional patterns).

The idea is to use databases instead of files and also do most of your
work in pure python in streaming fashion rather than using batch libraries
like pandas and other data frame libraries. Databases allow for operations 
like joins, ordering and simple aggregations without having to put 
everything in memory.

The idea of the library is part of a strategy 
to enable very productive proof of concepts on 
local resources (your laptop) which can migrate naturally and painlessly 
into production applications without extensive rewrites. 
Brunodb can be an efficient solution by itself for moderate data sizes.
Streaming pattern pipelines can be ported to Spark or some 
distributed cluster compute system fairly easily.

Brunodb frees you from some of the lower level details of dealing with 
these python database clients. It gives you any easy and natural way to 
schema and load data from either files or streams. It gives you some 
shortcuts for doing queries while also allowing you full SQL functionality 
when you need it. It makes working on either SQLite or Postgres the same.
And it allows for very fast bulk loads for Postgres by levering 
the dbcrossbar library.

There are no real dependencies besides sqlite3 which is a standard library 
module and pytest for running tests. psycopg2 and a postgres database is needed
to run the interface on postgres. dbcrossbar (easy to install rust library) 
is required for doing extremely fast bulk loads of postgres.

To install

pip install brunodb

See these for examples of how to use

Loading data:

https://github.com/dave31415/brunodb/blob/master/brunodb/cars_example.py

Querying data:

https://github.com/dave31415/brunodb/blob/master/test/test_cars.py

To run tests:

python -m pytest test

If you have postgres installed, you can test it as well. You'll need to put
the database password in the POSTGRES_PWD environment variable and have the
usual standards: running on localhost, usual port, user name postgres etc.

```
python -m pytest test_postscript 
```

If you install dbcrossbar you can do much faster postgres loads. 
Around 80X faster.

```
python -m pytest test_postgres_bulk_load
```

Or run all tests if you have postgres and dbcrossbar installed

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

Or add other config options:

```
config = {'db_type': 'postgres', 'port': 5555, 'password':'foo'}
dbase = DBase(config)
```
