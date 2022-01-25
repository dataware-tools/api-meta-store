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

    # Add secret column
    r = client.get(
        '/databases/default/config',
    )
    assert r.status_code == 200
    config = json.loads(r.text)
    config['columns'].append({
        'name': 'secret_column',
        'dtype': 'string',
        'aggregation': 'first',
        'display_name': 'secret_column',
        'is_secret': True,
    })
    client.patch(
        '/databases/default/config',
        json=config
    )


def add_record(database_id='default', record_id='pytest'):
    client.post(
        f'/databases/{database_id}/records',
        json={
            'record_id': record_id,
            'name': 'pytest',
            'description': 'Description',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2'],
            'secret_column': 'data',
            'path': ''
        }
    )


def remove_record(database_id='default', record_id='pytest'):
    client.delete(f'/databases/{database_id}/records/{record_id}')


@pytest.fixture
def add_data():
    _set_env()
    add_record(database_id='default', record_id='pytest')


def test_list_records_200(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/records',
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    _assert_list_response(data)


def test_list_records_404(init):
    _set_env()
    r = client.get(
        '/databases/unknown-database/records',
    )
    assert r.status_code == 404


def test_search_records_200(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/records',
        params={
            'search': 'pytest'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) > 0


def test_search_records_200_2(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/records',
        params={
            'search': 'record_id:pytest'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) > 0


def test_search_records_200_3(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/records',
        params={
            'search': 'record_id:pytest name:pytest'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) > 0


def test_search_records_200_4(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/records',
        params={
            'search': '"'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) == 0


def test_search_records_200_5(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/records',
        params={
            'search': 1
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) == 0


def test_search_records_400(init, add_data):
    _set_env()
    for keyword in ['+', ')', '(']:
        r = client.get(
            '/databases/default/records',
            params={
                'search': keyword
            }
        )
        assert r.status_code == 400


def test_sort_records_200(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/records',
        params={
            "sort_key": "description",
            "sort_order": -1
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) > 0


def test_sort_records_400(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/records',
        params={
            "sort_key": "unknown",
        }
    )
    assert r.status_code == 400


def test_fuzzy_search_records_200(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/records',
        params={
            'search': 'dEsC',
            'search_key': ['record_id', 'description']
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) > 0


def test_create_record_200(init):
    _set_env()
    r = client.post(
        '/databases/default/records',
        json={
            'record_id': 'pytest',
            'name': 'pytest',
            'description': 'Description',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        }
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert data['record_id'] == 'pytest'
    assert data['name'] == 'pytest'
    assert data['description'] == 'Description'
    r = client.get(
        '/databases/default/records',
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert any([d['record_id'] == 'pytest' for d in data['data']])


def test_create_record_200_2(init):
    _set_env()
    r = client.post(
        '/databases/default/records',
        json={
            'name': 'pytest2',
            'description': 'Description',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        }
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert 'record_id' in data.keys()
    assert data['name'] == 'pytest2'
    assert data['description'] == 'Description'
    r = client.get(
        '/databases/default/records',
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert any([d['name'] == 'pytest2' for d in data['data']])


def test_create_record_404(init):
    _set_env()
    r = client.post(
        '/databases/unknown-database/records',
        json={
            'record_id': 'pytest',
            'name': 'pytest',
            'description': 'Description',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        }
    )
    assert r.status_code == 404


def test_get_record_200(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/records/pytest',
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'record_id' in data.keys()
    assert data['record_id'] == 'pytest'
    assert data['description'] == 'Description'


def test_get_record_404(init, add_data):
    _set_dummy_env()
    r = client.get(
        '/databases/default/records/pytest',
    )
    assert r.status_code == 404


def test_update_record_200(init, add_data):
    _set_env()
    r = client.get(
        '/databases/default/records/pytest',
    )
    assert r.status_code == 200
    original_data = json.loads(r.text)
    r = client.patch(
        '/databases/default/records/pytest',
        json={
            'record_id': 'pytest',
            'description': 'new-description',
            'tag': 'new-tag'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'record_id' in data.keys()
    assert data['description'] == 'new-description'
    assert data['tag'] == 'new-tag'
    r = client.get(
        '/databases/default/records/pytest',
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert 'record_id' in data.keys()
    assert data['description'] == 'new-description'
    assert data['tag'] == 'new-tag'
    assert data['path'] == original_data['path']


def test_update_record_with_none(init, add_data):
    _set_env()
    r = client.patch(
        '/databases/default/records/pytest',
        json={
            'record_id': 'pytest',
            'description': 'new-description',
            'tag': None
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'record_id' in data.keys()
    assert data['description'] == 'new-description'
    assert data['tag'] is None
    r = client.get(
        '/databases/default/records/pytest',
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert 'record_id' in data.keys()
    assert data['description'] == 'new-description'
    assert data['tag'] is None


def test_update_record_404(init, add_data):
    _set_dummy_env()
    r = client.patch(
        '/databases/default/records/pytest',
        json={
            'record_id': 'pytest',
            'description': 'new-description',
            'tag': 'new-tag'
        }
    )
    assert r.status_code == 404


def test_patch_record_400(init, add_data):
    """The following request must be rejected
    as key `record_id` is used as one of `index_columns`

    """
    _set_env()
    r = client.patch(
        '/databases/default/records/pytest',
        json={
            'record_id': 'abc',
            'description': 'new-description',
            'tag': 'new-tag'
        }
    )
    assert r.status_code == 400


def test_patch_record_400_2(init, add_data):
    """The following request must be rejected
    as key `path` is not aggregated with `first`

    """
    _set_env()
    r = client.patch(
        '/databases/default/records/pytest',
        json={
            'record_id': 'pytest',
            'description': 'new-description',
            'tag': 'new-tag',
            'path': '/abc'
        }
    )
    assert r.status_code == 400


def test_delete_record_200(init, add_data):
    _set_env()
    r = client.delete(
        '/databases/default/records/pytest',
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'database_id' in data.keys()
    assert data['database_id'] == 'default'
    assert 'record_id' in data.keys()
    assert data['record_id'] == 'pytest'
    r = client.delete(
        '/databases/default/records/pytest',
    )
    assert r.status_code == 404


def test_delete_record_200_2(init, add_data):
    _set_env()

    # Add extra data
    client.post(
        '/databases/default/records',
        json={
            'record_id': 'pytest',
            'name': 'pytest',
            'path': '/path/to/file',
            'description': 'Description',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        }
    )

    # Delete the record
    r = client.delete(
        '/databases/default/records/pytest',
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'database_id' in data.keys()
    assert data['database_id'] == 'default'
    assert 'record_id' in data.keys()
    assert data['record_id'] == 'pytest'
    r = client.get(
        '/databases/default/records/pytest',
    )
    assert r.status_code == 404


def test_delete_record_404(init, add_data):
    _set_dummy_env()
    r = client.delete(
        '/databases/default/records/pytest',
    )
    assert r.status_code == 404
