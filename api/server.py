#!/usr/bin/env python
# Copyright API authors
"""The API server."""

import os
from typing import Optional

from dataware_tools_api_helper.helpers import get_jwt_payload_from_authorization
from fastapi import FastAPI, Header
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Metadata
description = "An API template."
terms_of_service = "http://tools.hdwlab.com/terms/"
contact = {
    "name": "API Support",
    "url": "http://tools.hdwlab.com/support",
    "email": "contact@hdwlab.co.jp",
}
license = {
    "name": "Apache 2.0",
    "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
}

# Initialize app
api = FastAPI(
    title="API Template",
    version="1.0",
    openapi="3.0.2",
    docs_route='/docs',
    description=description,
    terms_of_service=terms_of_service,
    contact=contact,
    license=license,
)
api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@api.get('/')
def index(authorization: Optional[str] = Header('')):
    """Index page."""
    jwt_payload = get_jwt_payload_from_authorization(authorization)
    res = {
        'jwt_payload': jwt_payload
    }
    return res


@api.get('/echo/{content}/{resp_type}')
def echo(content, resp_type):
    if resp_type == 'json':
        return {'content': content}
    else:
        return content


@api.get('/healthz')
def healthz():
    return 'ok'


if __name__ == '__main__':
    debug = os.environ.get('API_DEBUG', '') in ['true', 'True', 'TRUE', '1']
    print('Debug: {}'.format(debug))
    if debug:
        uvicorn.run(api, host="0.0.0.0", port=8080)
