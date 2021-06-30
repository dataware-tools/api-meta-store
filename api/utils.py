#!/usr/bin/env python
# Copyright API authors
"""Utilities."""

import datetime
import json
import os
import re
from typing import List

from dataware_tools_api_helper import get_forward_headers
from fastapi import Header, HTTPException
from pydtk.db import V4DBHandler as DBHandler
import requests

from api.exceptions import InvalidData, InvalidSortKey


def to_content_df(df, base_dir_path):
    """Return content_df.
    (Columns: record_id, path, content, msg_type, tag)

    Args:
        df (pandas.DataFrame): input data-frame
        base_dir_path (str): path-prefix

    Returns:
        (pandas.DataFrame): output data-frame

    """
    df = df[['record_id', 'path', 'contents', 'msg_type', 'tags']]
    df = df.rename(columns={"contents": "content", "tags": "tag"})
    df['tag'] = df['tag'].apply(lambda x: list(set(x.split(';'))))
    df['path'] = df['path'].apply(lambda x: os.path.join(base_dir_path, x))
    return df


def to_file_df(df, base_dir_path):
    """Return file_df.
    Columns: path, record_id, type, content_type, start_timestamp, end_timestamp

    Args:
        df (pandas.DataFrame): input data-frame
        base_dir_path (str): path-prefix

    Returns:
        (pandas.DataFrame): output data-frame

    """
    df = df[['path', 'record_id', 'data_type', 'content_type', 'start_timestamp', 'end_timestamp']]
    df = df.rename(columns={"data_type": "type", "content_type": "content-type"})
    df = df.groupby(['path'], as_index=False).agg({
        'record_id': 'first',
        'type': 'first',
        'content-type': 'first',
        'start_timestamp': 'min',
        'end_timestamp': 'max',
    })
    df['path'] = df['path'].apply(lambda x: os.path.join(base_dir_path, x))
    return df


def to_record_id_df(df):
    """Return record_id_df.
    Columns: 'record_id', 'duration', 'start_timestamp', 'end_timestamp', 'tags'

    Args:
        df (pandas.DataFrame): input data-frame

    Returns:
        (pandas.DataFrame): output data-frame

    """
    df = df[['record_id', 'start_timestamp', 'end_timestamp', 'tags']]
    df = df.groupby(['record_id'], as_index=False).agg({
        'start_timestamp': 'min',
        'end_timestamp': 'max',
        'tags': ';'.join
    })
    df['duration'] = df.end_timestamp - df.start_timestamp
    df['tags'] = df['tags'].apply(lambda x: list(set(x.split(';'))))
    return df


def _parse_search_keyword(keyword, expression, replace_expression=None):
    key = keyword.split(expression)[0]
    key = f"'{key}'"
    value = ''.join(keyword.split(expression)[1:])
    value = value.replace('\\', '')
    if key == '' or value == '':
        return ''
    if not value.isdecimal():
        value = f"'{value}'"
    if replace_expression == 'regex':
        return f"{key} == regex({value})"
    elif replace_expression:
        return f"{key} {replace_expression} {value}"
    return f"{key} {expression} {value}"


def parse_search_keyword(search_keyword: str, columns=None):
    """Parse search keywords and return a query.

    Args:
        search_keyword (str): Search keywords
        columns (list): The default search columns

    Return:
        (str): A query for where statement

    """
    if columns is None:
        columns = ['tags']

    if search_keyword is None:
        return None

    query = ''
    for idx, key in enumerate(search_keyword.split(' ')):
        if idx != 0:
            query += ' and '
        if ':' in key:
            query += _parse_search_keyword(key, ':', replace_expression='regex')
        elif '>=' in key:
            query += _parse_search_keyword(key, '>=')
        elif '<=' in key:
            query += _parse_search_keyword(key, '<=')
        elif '!=' in key:
            query += _parse_search_keyword(key, '!=')
        elif '=' in key:
            query += _parse_search_keyword(key, '=')
        elif '>' in key:
            query += _parse_search_keyword(key, '>')
        elif '<' in key:
            query += _parse_search_keyword(key, '<')
        else:
            filters = []
            for column in columns:
                filters.append(_parse_search_keyword('{0}:.*{1}.*'.format(column, key), ':',
                                                     replace_expression='regex'))
            query += '(' + ' or '.join(filters) + ')'

    return query


def escape_string(data: str, kind: str = None):
    """Escape string

    Args:
        data (str or None): input string
        kind (str): 'filtering', 'id'

    Returns:
        (str or None): escaped string

    """
    if data is None:
        return data

    escaped = data

    if kind is None:
        escaped = re.sub('[^a-zA-Z0-9:;.,_=<>" /~!@#$%^&()+-]', '', escaped)
    elif kind == 'filtering':
        escaped = re.sub('[^a-zA-Z0-9:;.,_=<>" /~!@#$%^&()+-]', '', escaped)
    elif kind == 'id':
        escaped = re.sub('[^a-zA-Z0-9_-]', '', escaped)
    elif kind == 'key':
        escaped = re.sub('[^a-zA-Z0-9_=<>/()@-]', '', escaped)
    elif kind == 'path':
        escaped = re.sub('[^a-zA-Z0-9:;.,_=<>/~!@#$%^&()+-]', '', escaped)
    elif kind == 'uuid':
        escaped = re.sub('[^a-zA-Z0-9_-]', '', escaped)
    else:
        pass

    return escaped


def validate_input_data(data: dict):
    """Validate the input data.

    Args:
        data (dict): input data

    """
    for k in data.keys():
        if k.startswith('_'):
            raise InvalidData(f'keys cannot start with "_": {k}')
        if k.startswith('.'):
            raise InvalidData(f'keys cannot start with ".": {k}')
        if escape_string(k, kind='key') != k:
            raise InvalidData(f'invalid key: {k}')


def filter_data(data: dict, excludes: List[str] = []) -> dict:
    """Filter data for response message.

    Args:
        data (dict): original data
        excludes (List[str], optional): keys of data to exclude from response

    Returns:
        (dict): data with filtered keys

    """
    return {k: v for k, v in data.items() if not k.startswith('_') and k not in excludes}


def get_db_handler(handler_type: str, database_id: str = None) -> DBHandler:
    """Returns DB-Handler.

    Args:
        handler_type (str): 'database', 'record', 'file' or 'config'
        database_id (str): database-id (optional)

    Returns:
        (DBHandler): Database handler

    """
    if handler_type == 'database':
        handler = DBHandler(
            db_class='database_id',
            read_on_init=False
        )
    elif handler_type == 'record':
        handler = DBHandler(
            db_class='meta',
            database_id=database_id,
            orient='record_id',
            read_on_init=False
        )
    elif handler_type == 'file':
        handler = DBHandler(
            db_class='meta',
            database_id=database_id,
            orient='path',
            read_on_init=False
        )
    elif handler_type == 'config':
        handler = DBHandler(
            db_class='meta',
            database_id=database_id,
            orient='path',
            read_on_init=False
        )
    else:
        raise ValueError(f'Unknown handler-type: {handler_type}')

    return handler


def validate_sort_key(sort_key: str, handler: DBHandler):
    """Validate sort-key.

    Args:
        sort_key (str): Sort key
        handler (DBHandler): PyDTK DB Handler

    """
    is_valid = sort_key in [c['name'] for c in handler.config['columns']]
    if not is_valid:
        raise InvalidSortKey(f'Sort-key "{sort_key}" is not available')


def generate_record_id():
    """Generate record_id.

    Returns:
        (str): Record ID

    """
    return datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')[:-3]


def get_secret_columns(database_id: str) -> List[str]:
    """Returns secret columns in the database.

    Args:
        database_id (str): Escaped database_id

    Returns:
        isecret_columns (List[str])

    """
    # Get config for database
    handler = get_db_handler('config', database_id=database_id)
    config = handler.config

    # Get list of secret columns
    secret_columns = []
    if 'columns' in config.keys():
        for column in config['columns']:
            if 'is_secret' in column.keys() and column['is_secret'] is True:
                secret_columns.append(column['name'])
    return secret_columns


class CheckPermissionClient:
    """Client for checking permission via api-permission-manager."""
    PERMISSION_MANAGER_SERVICE = os.environ.get(
        'PERMISSION_MANAGER_SERVICE',
        'https://demo.dataware-tools.com/api/latest/permission_manager',
    )

    def __init__(self, auth_header: str):
        """Initialize.

        Args:
            auth_header (str)

        """
        self.auth_header = auth_header

    def is_permitted(self, action_id: str, database_id: str) -> bool:
        """Check whether the action is permitted for the user on the database.

        Args:
            action_id (str): ID of an action to check permission.

        Returns:
            (bool): Whether if permitted or not.

        """
        try:
            res = requests.post(
                f'{self.PERMISSION_MANAGER_SERVICE}/permitted-actions/{action_id}',
                params={
                    'database_id': database_id,
                },
                headers={
                    'authorization': self.auth_header,
                },
            )
            res.raise_for_status()
            is_permitted = json.loads(res.text)
            return is_permitted
        except Exception:
            return False

    def columns_to_filter(self, database_id: str):
        """Get columns to filter for public only read users.

        Args:
            database_id (str)

        Returns:
            columns_to_filter (List[str])

        """
        if self.is_permitted('metadata:read', database_id):
            # Can read all
            columns_to_filter = []
        else:
            # Can read only public metadata
            columns_to_filter = get_secret_columns(database_id)
        return columns_to_filter


def get_check_permission_client(authorization: str = Header(None)):
    """FastAPI dependency for getting client for checking permission.

    Reference:
        - https://fastapi.tiangolo.com/tutorial/dependencies/

    """
    return CheckPermissionClient(authorization)

# TODO: Add client to override FastAPI dependency for running tests
# Reference: https://fastapi.tiangolo.com/advanced/testing-dependencies/
