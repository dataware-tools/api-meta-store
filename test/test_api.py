#!/usr/bin/env python
# Copyright API authors
"""Test code."""

import json

from api.server import api
from fastapi.testclient import TestClient

client = TestClient(api)


def test_healthz():
    r = client.get('/healthz')
    assert r.status_code == 200


def test_index():
    r = client.get('/', headers={"Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"})  # noqa: E501
    data = json.loads(r.text)
    assert 'jwt_payload' in data.keys()
    assert 'sub' in data['jwt_payload'].keys()
