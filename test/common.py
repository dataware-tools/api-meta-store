#!/usr/bin/env python
# Copyright API authors
"""Test common."""

import os

import api
from api.server import api as api_server
from fastapi.testclient import TestClient

client = TestClient(api_server)

base_dir = os.path.join(
    os.path.dirname(os.path.dirname(api.__file__)),
    'test',
    'assets',
    'db'
)


def _init_database():
    """Initialize database."""
    if os.path.exists(os.path.join(base_dir, 'default.json')):
        os.remove(os.path.join(base_dir, 'default.json'))
    if os.path.exists('/tmp/api-meta-store/default.json'):
        os.remove('/tmp/api-meta-store/default.json')


def _set_env():
    """Set environment for Testing."""
    os.environ['API_IGNORE_PERMISSION_CHECK'] = 'true'
    os.environ['PYDTK_META_DB_ENGINE'] = 'tinymongo'
    os.environ['PYDTK_META_DB_HOST'] = base_dir
    # os.environ['PYDTK_META_DB_ENGINE'] = 'mongodb'
    # os.environ['PYDTK_META_DB_HOST'] = ''
    # os.environ['PYDTK_META_DB_USERNAME'] = 'pytest'
    # os.environ['PYDTK_META_DB_PASSWORD'] = 'pytest'
    # os.environ['PYDTK_META_DB_DATABASE'] = 'pytest'


def _set_dummy_env():
    """Set dummy environment for Testing."""
    os.environ['API_IGNORE_PERMISSION_CHECK'] = 'true'
    os.environ['PYDTK_META_DB_ENGINE'] = 'tinymongo'
    os.environ['PYDTK_META_DB_HOST'] = '/tmp/api-meta-store'


def _assert_list_response(data):
    """Asserts list response."""
    assert 'data' in data.keys()
    assert 'sort_key' in data.keys()
    assert 'per_page' in data.keys()
    assert 'page' in data.keys()
    assert 'number_of_pages' in data.keys()
    assert 'total' in data.keys()
    assert 'length' in data.keys()
