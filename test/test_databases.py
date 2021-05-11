#!/usr/bin/env python
# Copyright API authors
"""Test code."""

import json
import os
import shutil

import api
from api.server import api as api_server
from fastapi.testclient import TestClient

client = TestClient(api_server)


# Initialize database
base_dir = os.path.join(os.path.dirname(os.path.dirname(api.__file__)), 'test', 'assets', 'test_v4')
shutil.copyfile(
    os.path.join(base_dir, 'default.original.json'),
    os.path.join(base_dir, 'default.json')
)


def _set_env():
    os.environ['PYDTK_META_DB_ENGINE'] = 'tinymongo'
    os.environ['PYDTK_META_DB_HOST'] = base_dir


def _set_dummy_env():
    os.environ['PYDTK_META_DB_ENGINE'] = 'tinymongo'
    os.environ['PYDTK_META_DB_HOST'] = '/tmp/api-meta-store'


def _assert_list_response(data):
    assert 'data' in data.keys()
    assert 'sort_key' in data.keys()
    assert 'count' in data.keys()
    assert 'per_page' in data.keys()
    assert 'page' in data.keys()
    assert 'number_of_pages' in data.keys()


def test_list_databases_200():
    _set_env()
    r = client.get('/databases')
    assert r.status_code == 200
    data = json.loads(r.text)
    _assert_list_response(data)
    assert len(data['data']) > 0


def test_create_database():
    _set_env()
    r = client.post(
        '/databases',
        json={
            'database_id': 'test',
            'name': 'test',
            'description': 'Description',
            'list': ['a', 'b', 'c']
        }
    )
    data = json.loads(r.text)
    assert data['database_id'] == 'test'
    assert data['name'] == 'test'
    assert data['description'] == 'Description'
    r = client.get('/databases')
    assert r.status_code == 200
    data = json.loads(r.text)
    assert any([d['database_id'] == 'test' for d in data['data']])


def test_get_database_200():
    _set_env()
    r = client.get('/databases/test')
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'database_id' in data.keys()
    assert data['database_id'] == 'test'
    assert data['description'] == 'Description'


def test_get_database_404():
    _set_dummy_env()
    r = client.get('/databases/test')
    assert r.status_code == 404


def test_update_database_200():
    _set_env()
    r = client.patch(
        '/databases/test',
        json={
            'database_id': 'test',
            'description': 'new-description',
            'tag': 'new-tag'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'database_id' in data.keys()
    assert data['description'] == 'new-description'
    assert data['tag'] == 'new-tag'
    r = client.get('/databases/test')
    data = json.loads(r.text)
    assert r.status_code == 200
    assert 'database_id' in data.keys()
    assert data['description'] == 'new-description'
    assert data['tag'] == 'new-tag'


def test_update_database_404():
    _set_dummy_env()
    r = client.patch(
        '/databases/test',
        json={
            'database_id': 'test',
            'description': 'new-description',
            'tag': 'new-tag'
        }
    )
    assert r.status_code == 404


def test_patch_database_400():
    _set_env()
    r = client.patch(
        '/databases/test',
        json={
            'database_id': 'abc',
            'description': 'new-description',
            'tag': 'new-tag'
        }
    )
    assert r.status_code == 400


def test_delete_database_200():
    _set_env()
    r = client.delete('/databases/test')
    assert r.status_code == 200
    r = client.delete('/databases/test')
    assert r.status_code == 404


def test_delete_database_404():
    _set_dummy_env()
    r = client.delete('/databases/test')
    assert r.status_code == 404
