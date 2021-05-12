#!/usr/bin/env python
# Copyright API authors
"""Test utilities."""


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
