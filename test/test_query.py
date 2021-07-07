from brunodb.query import get_query_sql, get_base_query, get_where_clause, get_order_by_sql


def standardize(string):
    string = ' '.join(string.split())
    return string.lower().strip()


def test_base_sql():
    table_name = 'foo'
    sql = get_base_query(table_name)
    assert standardize(sql) == "select * from foo"


def test_get_where_clause_nothing():
    kwargs = {}
    where_extra = ''
    where_clause, where_vals = get_where_clause(kwargs, where_extra=where_extra)
    assert where_clause == ''
    assert where_vals == []


def test_get_where_with_kwargs():
    kwargs = {'bar': 5}
    where_extra = ''
    where_clause, where_vals = get_where_clause(kwargs, where_extra=where_extra)
    where_clause = standardize(where_clause)

    assert where_clause == 'where bar = (?)'
    assert where_vals == [5]


def test_get_where_with_where_extra():
    kwargs = {}
    where_extra = 'buzz > 9'
    where_clause, where_vals = get_where_clause(kwargs, where_extra=where_extra)
    where_clause = standardize(where_clause)

    assert where_clause == 'where buzz > 9'
    assert where_vals == []


def test_get_where_with_kwargs_and_where_extra():
    kwargs = {'foo': 99, 'bar': 5}
    where_extra = 'buzz > 9'
    where_clause, where_vals = get_where_clause(kwargs, where_extra=where_extra)
    where_clause = standardize(where_clause)

    assert where_clause == 'where foo = (?) and bar = (?) and buzz > 9'
    assert where_vals == [99, 5]


def test_order_by_sql_nothing():
    order_by = ''
    order_by_sql = get_order_by_sql(order_by)
    assert order_by_sql == ''


def test_order_by_sql():
    order_by = 'foo'
    order_by_sql = get_order_by_sql(order_by)
    order_by_sql = standardize(order_by_sql)
    assert order_by_sql == 'order by foo'

    order_by = ['foo']
    order_by_sql = get_order_by_sql(order_by)
    order_by_sql = standardize(order_by_sql)
    assert order_by_sql == 'order by foo'


def test_order_by_sql_mult():
    order_by = ('foo', 'bar')

    order_by_sql = get_order_by_sql(order_by)
    order_by_sql = standardize(order_by_sql)
    assert order_by_sql == 'order by foo, bar'

    order_by = ['foo', 'bar']
    order_by_sql = get_order_by_sql(order_by)
    order_by_sql = standardize(order_by_sql)
    assert order_by_sql == 'order by foo, bar'


def test_order_by_sql_mult_desc():
    order_by = ('-foo', 'bar')

    order_by_sql = get_order_by_sql(order_by)
    order_by_sql = standardize(order_by_sql)
    assert order_by_sql == 'order by foo desc, bar'

    order_by = ['foo', '-bar']
    order_by_sql = get_order_by_sql(order_by)
    order_by_sql = standardize(order_by_sql)
    assert order_by_sql == 'order by foo, bar desc'


def test_query_sql():
    table_name = 'foo'
    sql, vals = get_query_sql(table_name)

    assert standardize(sql) == "select * from foo"
    assert vals == ()


def test_query_sql_complex():
    table_name = 'foo'
    fields = ['bar', 'buzz']
    kwargs = {'jug': 19,
              'bug': 'ant'}

    where_extra = 'hug <= 77'
    order_by = ('-buzz',)
    query_sql, vals = get_query_sql(table_name,
                                    fields=fields,
                                    where_extra=where_extra,
                                    count_table_rows=False,
                                    order_by=order_by,
                                    **kwargs)

    sql = standardize(query_sql)
    expected = "select bar, buzz from foo where jug = (?) and bug = (?) and hug <= 77 order by buzz desc"

    assert sql == expected
    assert vals == (19, 'ant')


def test_query_sql_complex():
    table_name = 'foo'
    fields = ['bar', 'buzz']
    kwargs = {'jug': 19,
              'bug': 'ant'}

    where_extra = 'hug <= 77'
    order_by = ('-buzz',)
    query_sql, vals = get_query_sql(table_name,
                                    where_extra=where_extra,
                                    count_table_rows=True,
                                    order_by=order_by,
                                    **kwargs)

    sql = standardize(query_sql)
    expected = "select count(*) from foo where jug = (?) and bug = (?) and hug <= 77 order by buzz desc"

    assert sql == expected
    assert vals == (19, 'ant')
