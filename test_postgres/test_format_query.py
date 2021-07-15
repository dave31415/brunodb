import pytest
from brunodb.format_query import format_query_check_chars, format_sql_in_context
from brunodb.postgres_utils import open_connection


def test_format_check_chars():
    template = "select {field} from {table}"

    params = {'field': 'name', 'table': 'customers'}
    result = format_query_check_chars(template, params)
    expected = "select name from customers"
    assert result == expected

    with pytest.raises(ValueError):
        # raises an error
        params = {'field': "-*'", 'table': "'-"}
        result = format_query_check_chars(template, params)
        expected = "select -*' from '-"
        assert result == expected


def test_format_in_context_sqlite():
    template = "select {field} from {table}"

    params = {'field': 'name', 'table': 'customers'}
    result = format_sql_in_context(template, params, None)
    expected = "select name from customers"
    assert result == expected

    with pytest.raises(ValueError):
        # raises an error
        params = {'field': "-*'", 'table': "'-"}
        result = format_sql_in_context(template, params, None)
        expected = "select -*' from '-"
        assert result == expected


def test_format_in_context_postgres():
    conn = open_connection()

    template = "select {field} from {table}"

    params = {'field': 'name', 'table': 'customers'}
    result = format_sql_in_context(template, params, conn)
    expected = 'select "name" from "customers"'
    assert result == expected
