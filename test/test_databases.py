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


@pytest.fixture
def add_data():
    _set_env()
    add_database('default')


def test_list_databases_200(init, add_data):
    _set_env()
    r = client.get('/databases')
    assert r.status_code == 200
    data = json.loads(r.text)
    _assert_list_response(data)
    assert len(data['data']) > 0


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
            'database_id': 'default',
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
