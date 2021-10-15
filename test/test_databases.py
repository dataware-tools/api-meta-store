#!/usr/bin/env python
# Copyright API authors
"""Test code."""

import json

import pytest
from .common import _init_database, _set_env, _set_dummy_env, client, _assert_list_response


@pytest.fixture
def init():
    _init_database()


def add_database(database_id='default'):
    client.post(
        '/databases',
        json={
            'database_id': database_id,
            'name': 'default',
            'description': 'Description',
            'list': ['a', 'b', 'c']
        }
    )


def remove_database(database_id='default'):
    client.delete(f'/databases/{database_id}')


@pytest.fixture
def add_data():
    _set_env()
    add_database('default')


@pytest.fixture
def add_multiple_data():
    _set_env()
    add_database('default')
    add_database('database1')
    add_database('database2')
    add_database('database3')
    add_database('database4')


def test_list_databases_200(init, add_data):
    _set_env()
    r = client.get('/databases')
    assert r.status_code == 200
    data = json.loads(r.text)
    _assert_list_response(data)
    assert len(data['data']) > 0


def test_list_databases_with_pagination_200(init, add_multiple_data):
    _set_env()
    # Get first page
    r = client.get('/databases', params={
        'per_page': 3,
        'page': 1,
    })
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) == 3

    # Get second page
    r = client.get('/databases', params={
        'per_page': 3,
        'page': 2,
    })
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) == 2

    # Get third page
    r = client.get('/databases', params={
        'per_page': 3,
        'page': 3,
    })
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) == 0


def test_fuzzy_search_databases_200(init, add_data):
    _set_env()
    r = client.get('/databases', params={
        'search': 'def'
    })
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) > 0


def test_fuzzy_search_databases_200_2(init, add_data):
    _set_env()
    r = client.get('/databases', params={
        'search': 'def',
        'search_key': ['abc']
    })
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) == 0


def test_fuzzy_search_databases_200_3(init, add_data):
    _set_env()
    r = client.get('/databases', params={
        'search': 'def',
        'search_key': ['abc', 'database_id']
    })
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) > 0


def test_fuzzy_search_databases_200_4(init, add_data):
    _set_env()
    r = client.get('/databases', params={
        'search': 'Desc',
        'search_key': ['description']
    })
    assert r.status_code == 200
    data = json.loads(r.text)
    assert len(data['data']) > 0


def test_search_databases_400(init, add_data):
    _set_env()
    for keyword in ['+', ')', '(']:
        r = client.get('/databases', params={
            'search': keyword,
        })
        assert r.status_code == 400


def test_create_database(init):
    _set_env()
    r = client.get('/databases/default')
    if r.status_code == 200:
        return
    r = client.post(
        '/databases',
        json={
            'database_id': 'default',
            'name': 'default',
            'description': 'Description',
            'list': ['a', 'b', 'c']
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert data['database_id'] == 'default'
    assert data['name'] == 'default'
    assert data['description'] == 'Description'
    r = client.get('/databases')
    assert r.status_code == 200
    data = json.loads(r.text)
    assert any([d['database_id'] == 'default' for d in data['data']])


def test_get_database_200(init, add_data):
    _set_env()
    r = client.get('/databases/default')
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'database_id' in data.keys()
    assert data['database_id'] == 'default'
    assert data['description'] == 'Description'


def test_get_database_404(init, add_data):
    _set_dummy_env()
    r = client.get('/databases/default')
    assert r.status_code == 404


def test_update_database_200(init, add_data):
    _set_env()
    r = client.patch(
        '/databases/default',
        json={
            'description': 'new-description',
            'tag': 'new-tag'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'database_id' in data.keys()
    assert data['description'] == 'new-description'
    assert data['tag'] == 'new-tag'
    r = client.get('/databases/default')
    data = json.loads(r.text)
    assert r.status_code == 200
    assert 'database_id' in data.keys()
    assert data['description'] == 'new-description'
    assert data['tag'] == 'new-tag'


def test_update_database_404(init, add_data):
    _set_dummy_env()
    r = client.patch(
        '/databases/default',
        json={
            'database_id': 'default',
            'description': 'new-description',
            'tag': 'new-tag'
        }
    )
    assert r.status_code == 404


def test_patch_database_400(init, add_data):
    _set_env()
    r = client.patch(
        '/databases/default',
        json={
            'database_id': 'abc',
            'description': 'new-description',
            'tag': 'new-tag'
        }
    )
    assert r.status_code == 400


def test_delete_database_200(init, add_data):
    _set_env()
    r = client.delete('/databases/default')
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'database_id' in data.keys()
    assert data['database_id'] == 'default'
    r = client.delete('/databases/default')
    assert r.status_code == 404


def test_delete_database_404(init, add_data):
    _set_dummy_env()
    r = client.delete('/databases/default')
    assert r.status_code == 404
