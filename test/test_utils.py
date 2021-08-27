#!/usr/bin/env python
# Copyright API authors
"""Test utilities."""
import pytest
from .common import _init_database, _set_env, client
from .test_config import _get_config
from .test_databases import add_database


@pytest.fixture
def init():
    _init_database()
    _set_env()
    add_database('default')


def test_validate_input_data():
    """Test for validate_input_data."""
    from api.utils import validate_input_data
    from api.exceptions import InvalidData
    data = {
        'aaa': 'bbb'
    }
    validate_input_data(data)

    data = {
        '_aaa': 'bbb'
    }
    try:
        validate_input_data(data)
        raise Exception
    except InvalidData:
        pass


def test_filter_data():
    """Test for filter_data."""
    from api.utils import filter_data
    data = {
        'aaa': 'bbb',
        '_aaa': 'bbb'
    }

    filtered_data = filter_data(data)
    assert 'aaa' in filtered_data.keys()
    assert '_aaa' not in filtered_data.keys()


def test_filter_data_excludes():
    """Test for filter_data with exclude option."""
    from api.utils import filter_data
    data = {
        'exclude1': 'exclude1',
        'exclude2': 'exclude2',
        'include': 'include',
    }

    filtered_data = filter_data(data, excludes=['exclude1', 'exclude2'])
    assert 'include' in filtered_data.keys()
    assert 'exclude1' not in filtered_data.keys()
    assert 'exclude2' not in filtered_data.keys()


def test_parse_search_keyword():
    """Test `parse_search_keyword`."""
    from api.utils import parse_search_keyword

    assert parse_search_keyword('abc:def') == "'abc' == regex('def')"
    assert parse_search_keyword('time>0') == "'time' > 0"
    assert parse_search_keyword('time<0') == "'time' < 0"
    assert parse_search_keyword('time>=0') == "'time' >= 0"
    assert parse_search_keyword('time<=0') == "'time' <= 0"
    assert parse_search_keyword('time=0') == "'time' == 0"
    assert parse_search_keyword('boolean:true') == "'boolean' == True"
    assert parse_search_keyword('boolean:False') == "'boolean' == False"
