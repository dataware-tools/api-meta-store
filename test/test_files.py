#!/usr/bin/env python
# Copyright API authors
"""Test code."""

import json

import pytest
from .common import _init_database, _set_env, _set_dummy_env, client, _assert_list_response


@pytest.fixture
def init():
    _init_database()


@pytest.fixture
def add_data():
    _set_env()
    client.post(
        '/files',
        json={
            'name': 'pytest',
            'description': 'Description',
            'path': '/path/to/file.abc',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        },
        params={
            'database_id': 'default',
            'record_id': 'pytest'
        }
    )


def test_list_files_200(init):
    _set_env()
    r = client.get(
        '/files',
        params={
            'database_id': 'default',
            'record_id': 'test'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    _assert_list_response(data)


def test_list_files_200_2(init):
    _set_env()
    r = client.get(
        '/files',
        params={
            'database_id': 'default',
            'record_id': 'test',
            'search': '.json'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    _assert_list_response(data)
    assert len(data['data']) > 0
    for item in data['data']:
        assert '.json' in item['path']


def test_list_files_404(init):
    _set_dummy_env()
    r = client.get(
        '/files',
        params={
            'database_id': 'default',
            'record_id': 'test'
        }
    )
    assert r.status_code == 404


def test_create_file_200(init):
    _set_env()
    r = client.post(
        '/files',
        json={
            'name': 'pytest',
            'description': 'Description',
            'path': '/path/to/file.abc',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        },
        params={
            'database_id': 'default',
            'record_id': 'pytest'
        }
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert data['record_id'] == 'pytest'
    assert data['name'] == 'pytest'
    assert data['description'] == 'Description'
    assert data['path'] == '/path/to/file.abc'
    r = client.get(
        '/files',
        params={
            'database_id': 'default',
            'record_id': 'pytest'
        }
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert any([d['path'] == '/path/to/file.abc' for d in data['data']])


def test_create_file_404(init):
    _set_dummy_env()
    r = client.post(
        '/files',
        json={
            'name': 'pytest',
            'description': 'Description',
            'path': '/path/to/file.abc',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        },
        params={
            'database_id': 'default',
            'record_id': 'pytest'
        }
    )
    assert r.status_code == 404


def test_get_file_200(init, add_data):
    _set_env()
    r = client.get(
        '/files/path/to/file.abc',
        params={
            'database_id': 'default',
            'record_id': 'pytest'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'record_id' in data.keys()
    assert data['record_id'] == 'pytest'
    assert data['description'] == 'Description'


def test_get_file_404(init, add_data):
    _set_dummy_env()
    r = client.get(
        '/files/path/to/file.abc',
        params={
            'database_id': 'default',
            'record_id': 'pytest'
        }
    )
    assert r.status_code == 404


def test_update_file_200(init, add_data):
    _set_env()
    r = client.patch(
        '/files/path/to/file.abc',
        json={
            'description': 'new-description',
            'tag': 'new-tag'
        },
        params={
            'database_id': 'default',
            'record_id': 'pytest'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert data['description'] == 'new-description'
    assert data['tag'] == 'new-tag'
    r = client.get(
        '/files/path/to/file.abc',
        params={
            'database_id': 'default',
            'record_id': 'pytest'
        }
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert 'record_id' in data.keys()
    assert data['description'] == 'new-description'
    assert data['tag'] == 'new-tag'


def test_update_file_404(init, add_data):
    _set_dummy_env()
    r = client.patch(
        '/files/path/to/file.abc',
        json={
            'description': 'new-description',
            'tag': 'new-tag'
        },
        params={
            'database_id': 'default',
            'record_id': 'pytest'
        }
    )
    assert r.status_code == 404


def test_patch_file_400(init, add_data):
    _set_env()
    r = client.patch(
        '/files/path/to/file.abc',
        json={
            'path': 'abc',
            'description': 'new-description',
            'tag': 'new-tag'
        },
        params={
            'database_id': 'default',
            'record_id': 'pytest'
        }
    )
    assert r.status_code == 400


def test_delete_file_200(init, add_data):
    _set_env()
    r = client.delete(
        '/files/path/to/file.abc',
        params={
            'database_id': 'default',
            'record_id': 'pytest'
        }
    )
    assert r.status_code == 200
    r = client.delete(
        '/files/path/to/file.abc',
        params={
            'database_id': 'default',
            'record_id': 'pytest'
        }
    )
    assert r.status_code == 404


def test_delete_record_404():
    _set_dummy_env()
    r = client.delete(
        '/records/pytest',
        params={
            'database_id': 'default'
        }
    )
    assert r.status_code == 404
