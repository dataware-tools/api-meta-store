#!/usr/bin/env python
# Copyright API authors
"""Test code."""

import json

import pytest
from .common import _init_database, _set_env, _set_dummy_env, client, _assert_list_response
from .test_databases import add_database


@pytest.fixture
def init():
    _init_database()
    _set_env()
    add_database('default')


def add_file(database_id='default', record_id='pytest', path='/path/to/file.abc'):
    client.post(
        f'/databases/{database_id}/files',
        json={
            'name': 'pytest',
            'description': 'Description',
            'record_id': record_id,
            'path': path,
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        }
    )


def remove_file_by_uuid(database_id='default', uuid=''):
    client.delete(f'/databases/{database_id}/files/{uuid}')


def remove_file_by_path(database_id='default', record_id='pytest', path='/path/to/file.abc'):
    r = client.get(
        f'/databases/{database_id}/files',
        json={
            'record_id': record_id,
            'search': path
        }
    )
    if r.status_code == 200:
        data = json.loads(r)
        for file in data['data']:
            uuid = file['uuid']
            client.delete(f'/databases/{database_id}/files/{uuid}')


@pytest.fixture
def add_data():
    _set_env()
    add_file(database_id='default', path='/path/to/file.abc')


def _get_uuid():
    _set_env()
    r = client.get(
        '/databases/default/files',
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    return data['data'][0]['uuid']


def test_list_files_200(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/files',
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    _assert_list_response(data)


def test_list_files_200_1(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/files',
        params={
            'record_id': 'pytest'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    _assert_list_response(data)


def test_list_files_200_2(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/files',
        params={
            'record_id': 'pytest',
            'search': '.abc'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    _assert_list_response(data)
    assert len(data['data']) > 0
    for item in data['data']:
        assert '.abc' in item['path']
        assert 'uuid' in item.keys()


def test_list_files_404(init):
    _set_dummy_env()
    r = client.get(
        '/databases/default/files',
        params={
            'record_id': 'pytest'
        }
    )
    assert r.status_code == 404


def test_fuzzy_search_files_200(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/files',
        params={
            'search': 'pytest',
            'search_key': ['name']
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) > 0


def test_create_file_200(init):
    _set_env()
    r = client.post(
        '/databases/default/files',
        json={
            'name': 'pytest',
            'record_id': 'pytest',
            'description': 'Description',
            'path': '/path/to/file.abc',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        }
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert data['record_id'] == 'pytest'
    assert data['name'] == 'pytest'
    assert data['description'] == 'Description'
    assert 'uuid' in data.keys()
    assert data['path'] == '/path/to/file.abc'
    r = client.get(
        '/databases/default/files',
        params={
            'record_id': 'pytest'
        }
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert any([d['path'] == '/path/to/file.abc' for d in data['data']])


def test_create_file_404(init):
    _set_dummy_env()
    r = client.post(
        '/databases/default/files',
        json={
            'name': 'pytest',
            'record_id': 'pytest',
            'description': 'Description',
            'path': '/path/to/file.abc',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        }
    )
    assert r.status_code == 404


def test_get_file_200(init, add_data):
    uuid = _get_uuid()
    _set_env()
    r = client.get(
        '/databases/default/files/{}'.format(uuid),
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'record_id' in data.keys()
    assert 'path' in data.keys()
    assert 'uuid' in data.keys()
    assert data['record_id'] == 'pytest'
    assert data['description'] == 'Description'


def test_get_file_404(init, add_data):
    uuid = _get_uuid()
    _set_dummy_env()
    r = client.get(
        '/databases/default/files/{}'.format(uuid),
    )
    assert r.status_code == 404


def test_update_file_200(init, add_data):
    uuid = _get_uuid()
    _set_env()
    r = client.patch(
        '/databases/default/files/{}'.format(uuid),
        json={
            'description': 'new-description',
            'tag': 'new-tag'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert data['description'] == 'new-description'
    assert data['tag'] == 'new-tag'
    r = client.get(
        '/databases/default/files/{}'.format(uuid),
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert 'record_id' in data.keys()
    assert data['description'] == 'new-description'
    assert data['tag'] == 'new-tag'


def test_update_file_404(init, add_data):
    uuid = _get_uuid()
    _set_dummy_env()
    r = client.patch(
        '/databases/default/files/{}'.format(uuid),
        json={
            'description': 'new-description',
            'tag': 'new-tag'
        }
    )
    assert r.status_code == 404


def test_patch_file_400(init, add_data):
    uuid = _get_uuid()
    _set_env()
    r = client.patch(
        '/databases/default/files/{}'.format(uuid),
        json={
            'path': 'abc',
            'description': 'new-description',
            'tag': 'new-tag'
        }
    )
    assert r.status_code == 400


def test_delete_file_200(init, add_data):
    uuid = _get_uuid()
    _set_env()
    r = client.delete(
        '/databases/default/files/{}'.format(uuid),
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'database_id' in data.keys()
    assert data['database_id'] == 'default'
    assert 'uuid' in data.keys()
    assert data['uuid'] == uuid
    r = client.delete(
        '/databases/default/files/{}'.format(uuid),
    )
    assert r.status_code == 404


def test_delete_record_404():
    uuid = 'abcdef'
    _set_dummy_env()
    r = client.delete(
        '/databases/default/files/{}'.format(uuid),
    )
    assert r.status_code == 404


def test_delete_record_404_2():
    uuid = '0%0A'
    _set_dummy_env()
    r = client.delete(
        '/databases/default/files/{}'.format(uuid),
    )
    assert r.status_code == 404
