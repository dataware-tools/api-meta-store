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


def test_escape_string():
    """Test for escape_string."""
    from api.utils import escape_string

    assert escape_string('record_id:abc name:def', kind='filtering') == 'record_id:abc name:def'
    assert escape_string('abc-def1234;[+"', kind='id') == 'abc-def1234'
    assert escape_string('/rosbag/topic@#$%', kind='key') == '/rosbag/topic@'
    assert escape_string('/path/to/file.ext', kind='path') == '/path/to/file.ext'
    assert escape_string('38123[[F9I{)(UFOIU#Y&(!', kind='uuid') == '38123F9IUFOIUY'
