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
        '/records',
        json={
            'record_id': 'pytest',
            'name': 'pytest',
            'description': 'Description',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        },
        params={
            'database_id': 'default'
        }
    )


def test_list_records_200(init):
    _set_env()
    r = client.get(
        '/records',
        params={
            'database_id': 'default'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    _assert_list_response(data)


def test_list_records_404(init):
    _set_env()
    r = client.get(
        '/records',
        params={
            'database_id': 'unknown-database'
        }
    )
    assert r.status_code == 404


def test_create_record_200(init):
    _set_env()
    r = client.post(
        '/records',
        json={
            'record_id': 'pytest',
            'name': 'pytest',
            'description': 'Description',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        },
        params={
            'database_id': 'default'
        }
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert data['record_id'] == 'pytest'
    assert data['name'] == 'pytest'
    assert data['description'] == 'Description'
    r = client.get(
        '/records',
        params={
            'database_id': 'default'
        }
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert any([d['record_id'] == 'pytest' for d in data['data']])


def test_create_record_404(init):
    _set_env()
    r = client.post(
        '/records',
        json={
            'record_id': 'pytest',
            'name': 'pytest',
            'description': 'Description',
            'list': ['a', 'b', 'c'],
            'tags': ['tag1', 'tag2']
        },
        params={
            'database_id': 'aaa'
        }
    )
    assert r.status_code == 404


def test_get_record_200(init, add_data):
    _set_env()
    r = client.get(
        '/records/pytest',
        params={
            'database_id': 'default',
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'record_id' in data.keys()
    assert data['record_id'] == 'pytest'
    assert data['description'] == 'Description'


def test_get_record_404(init, add_data):
    _set_dummy_env()
    r = client.get(
        '/records/pytest',
        params={
            'database_id': 'default',
        }
    )
    assert r.status_code == 404


def test_update_record_200(init, add_data):
    _set_env()
    r = client.patch(
        '/records/pytest',
        json={
            'record_id': 'pytest',
            'description': 'new-description',
            'tag': 'new-tag'
        },
        params={
            'database_id': 'default'
        }
    )
    assert r.status_code == 200
    data = json.loads(r.text)
    assert 'record_id' in data.keys()
    assert data['description'] == 'new-description'
    assert data['tag'] == 'new-tag'
    r = client.get(
        '/records/pytest',
        params={
            'database_id': 'default'
        }
    )
    data = json.loads(r.text)
    assert r.status_code == 200
    assert 'record_id' in data.keys()
    assert data['description'] == 'new-description'
    assert data['tag'] == 'new-tag'


def test_update_record_404(init, add_data):
    _set_dummy_env()
    r = client.patch(
        '/records/pytest',
        json={
            'record_id': 'pytest',
            'description': 'new-description',
            'tag': 'new-tag'
        },
        params={
            'database_id': 'default'
        }
    )
    assert r.status_code == 404


def test_patch_record_400(init, add_data):
    _set_env()
    r = client.patch(
        '/records/pytest',
        json={
            'record_id': 'abc',
            'description': 'new-description',
            'tag': 'new-tag'
        },
        params={
            'database_id': 'default'
        }
    )
    assert r.status_code == 400


def test_patch_record_400_2(init, add_data):
    _set_env()
    r = client.patch(
        '/records/pytest',
        json={
            'record_id': 'pytest',
            'description': 'new-description',
            'tag': 'new-tag',
            'tags': []
        },
        params={
            'database_id': 'default'
        }
    )
    assert r.status_code == 400


def test_delete_record_200(init, add_data):
    _set_env()
    r = client.delete(
        '/records/pytest',
        params={
            'database_id': 'default'
        }
    )
    assert r.status_code == 200
    r = client.delete(
        '/records/pytest',
        params={
            'database_id': 'default'
        }
    )
    assert r.status_code == 404


def test_delete_record_404(init, add_data):
    _set_dummy_env()
    r = client.delete(
        '/records/pytest',
        params={
            'database_id': 'default'
        }
    )
    assert r.status_code == 404
