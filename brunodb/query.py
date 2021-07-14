

def get_base_query(table_name,
                   fields=None,
                   count_table_rows=False):

    if count_table_rows:
        assert fields is None
        field_string = 'COUNT(*)'
    else:
        if fields is None or fields == '*':
            field_string = '*'
        else:
            field_string = ', '.join(fields)

    sql = "SELECT %s FROM %s" % (field_string, table_name)
    return sql


def get_where_clause(kwargs, where_extra='', place_holder='?'):

    if not kwargs and not where_extra:
        return '', []

    where_vals = []
    where = []

    where_sql = ''

    for key, value in kwargs.items():
        where.append('%s = (%s)' % (key, place_holder))
        where_vals.append(value)

    if len(where) > 0 or where_extra:
        where_sql = where_sql + ' WHERE '

    if len(where) > 0:
        where_string = ' AND '.join(where)
        where_sql += where_string
    else:
        where_string = ''

    if where_extra:
        if where_string:
            where_sql += ' AND ' + where_extra
        else:
            where_sql += ' ' + where_extra

    return where_sql, where_vals


def get_order_by_sql(order_by_sequence):
    if not order_by_sequence:
        return ''

    if isinstance(order_by_sequence, list):
        order_by_seq = order_by_sequence
    elif isinstance(order_by_sequence, tuple):
        order_by_seq = list(order_by_sequence)
    else:
        order_by_seq = [order_by_sequence]

    order_by_list = []

    for ob in order_by_seq:
        if ob.startswith('-'):
            ob = ob[1:] + ' DESC'

        order_by_list.append(ob)

    order_by_sql = ', '.join(order_by_list)
    order_by_sql = ' ORDER BY %s' % order_by_sql
    return order_by_sql.strip()


def get_query_sql(table_name,
                  fields=None,
                  where_extra=None,
                  count_table_rows=False,
                  order_by=None,
                  place_holder='?',
                  **kwargs):

    base_sql = get_base_query(table_name,
                              fields=fields,
                              count_table_rows=count_table_rows)

    sql = base_sql
    vals = []

    where_sql, where_vals = get_where_clause(kwargs, where_extra=where_extra, place_holder=place_holder)
    sql = ("%s %s" % (sql, where_sql)).strip()
    vals += where_vals

    order_by_sql = get_order_by_sql(order_by)
    sql = ("%s %s" % (sql, order_by_sql)).strip()

    return sql, tuple(vals)
