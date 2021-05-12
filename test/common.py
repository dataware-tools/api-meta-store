#!/usr/bin/env python
# Copyright API authors
"""Test common."""

import os
import shutil

import api
from api.server import api as api_server
from fastapi.testclient import TestClient

client = TestClient(api_server)

base_dir = os.path.join(
    os.path.dirname(os.path.dirname(api.__file__)),
    'test',
    'assets',
    'test_v4'
)


def _init_database():
    """Initialize database."""
    shutil.copyfile(
        os.path.join(base_dir, 'default.original.json'),
        os.path.join(base_dir, 'default.json')
    )


def _set_env():
    """Set environment for PyDTK."""
    os.environ['PYDTK_META_DB_ENGINE'] = 'tinymongo'
    os.environ['PYDTK_META_DB_HOST'] = base_dir


def _set_dummy_env():
    """Set dummy environment for PyDTK."""
    os.environ['PYDTK_META_DB_ENGINE'] = 'tinymongo'
    os.environ['PYDTK_META_DB_HOST'] = '/tmp/api-meta-store'


def _assert_list_response(data):
    """Asserts list response."""
    assert 'data' in data.keys()
    assert 'sort_key' in data.keys()
    assert 'count' in data.keys()
    assert 'per_page' in data.keys()
    assert 'page' in data.keys()
    assert 'number_of_pages' in data.keys()
