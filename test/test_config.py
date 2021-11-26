#!/usr/bin/env python
# Copyright API authors
"""Test code."""

import json

import pytest
from .common import _init_database, _set_env, _set_dummy_env, client
from .test_databases import add_database


@pytest.fixture
def init():
    _init_database()
    _set_env()
    add_database('default')


@pytest.fixture
def add_data():
    _set_env()
    client.post(
        '/databases/default/records',
        json={
            'record_id': 'pytest',
            'name': 'pytest',
            'path': '',
            'description': 'Description',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        }
    )


def _get_config():
    _set_env()
    r = client.get(
        '/databases/default/config',
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    return data


def test_get_config_200(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/config',
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'columns' in data.keys()
    for column in data['columns']:
        assert 'name' in column.keys()
        assert 'dtype' in column.keys()
        assert 'aggregation' in column.keys()
        assert 'display_name' in column.keys()


def test_get_config_404(init, add_data):
    _set_dummy_env()
    r = client.get(
        '/databases/default/config',
    )
    assert r.status_code == 404


def test_update_config_200(init, add_data):
    config = _get_config()
    _set_env()
    r = client.patch(
        '/databases/default/config',
        json=config
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert str(config) == str(data)


def test_update_config_200_2(init, add_data):
    config = _get_config()
    for column in config['columns']:
        column['display_name'] = 'display_name'

    _set_env()
    r = client.patch(
        '/databases/default/config',
        json=config
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert str(config) == str(data)


def test_update_config_200_3(init, add_data):
    config = _get_config()
    config['columns'].append({
        'name': 'abc',
        'dtype': 'string',
        'aggregation': 'first',
        'display_name': 'ABC'
    })

    _set_env()
    r = client.patch(
        '/databases/default/config',
        json=config
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert str(config) == str(data)


def test_update_config_datetime(init, add_data):
    config = _get_config()
    config['columns'].append({
        'name': 'datetime',
        'dtype': 'datetime',
        'aggregation': 'first',
        'display_name': 'Datetime'
    })

    _set_env()
    r = client.patch(
        '/databases/default/config',
        json=config
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert str(config) == str(data)

    r = client.post(
        '/databases/default/files',
        json={
            'name': 'pytest',
            'record_id': 'pytest',
            'path': '/path/to/file.datetime',
            'datetime': '2019-12-04 12:34:56.789'        # ISO format is OK
        }
    )
    assert r.status_code == 200
    r = client.get(
        '/databases/default/files',
        params={
            'record_id': 'pytest'
        }
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert any([
        d['path'] == '/path/to/file.datetime' and 'datetime'
        in d.keys() for d in data['data']]
    )

    r = client.post(
        '/databases/default/files',
        json={
            'name': 'pytest',
            'record_id': 'pytest',
            'path': '/path/to/file.datetime',
            'datetime': 12345678.9        # Timestamp (Unix-time) is OK
        }
    )
    assert r.status_code == 200

    r = client.post(
        '/databases/default/files',
        json={
            'name': 'pytest',
            'record_id': 'pytest',
            'path': '/path/to/file.datetime',
            'datetime': '2019/12/04'        # non ISO-format is NG
        }
    )
    assert r.status_code == 400


def test_update_config_400(init, add_data):
    _set_env()
    r = client.patch(
        '/databases/default/config',
        json={
            'columns': []
        }
    )
    assert r.status_code == 400


def test_update_config_400_2(init, add_data):
    config = _get_config()
    for column in config['columns']:
        column['name'] = ''

    _set_env()
    r = client.patch(
        '/databases/default/config',
        json=config
    )
    assert r.status_code == 400


def test_update_config_404(init, add_data):
    _set_dummy_env()
    r = client.patch(
        '/databases/default/config',
        json={}
    )
    assert r.status_code == 404
