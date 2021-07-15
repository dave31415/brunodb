from brunodb.database_sqlite import DBaseSqlite
try:
    from brunodb.database_postgres import DBasePostgres
except ImportError as e:
    def DBasePostgres(*_, **__):
        # only raise the import error if it is actually called
        raise e


def DBase(config):
    if config['db_type'] == 'sqlite':
        filename = config.get('filename')
        isolation_level = config.get('isolation_level')
        journal_model = config.get('journal_mode')
        return DBaseSqlite(filename, isolation_level=isolation_level, journal_mode=journal_model)
    elif config['db_type'] == 'postgres':
        return DBasePostgres(config)
    else:
        raise ValueError('Unknown db_type: %s' % config['db_type'])
