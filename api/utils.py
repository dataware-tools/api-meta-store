#!/usr/bin/env python
# Copyright API authors
"""Utilities."""

import datetime
from distutils.util import strtobool
import os
from typing import List

from dataware_tools_api_helper.helpers import escape_string
from dataware_tools_api_helper.permissions import CheckPermissionClient, DummyCheckPermissionClient
from fastapi import Header
from pydtk.db import V4DBHandler as DBHandler


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
    if value in ['true', 'True', 'false', 'False']:
        value = value.capitalize()
        return f"{key} == {value}"
    else:
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

    if search_keyword in [None, ""]:
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
            query += _parse_search_keyword(key, '=', replace_expression='==')
        elif '>' in key:
            query += _parse_search_keyword(key, '>')
        elif '<' in key:
            query += _parse_search_keyword(key, '<')
        else:
            filters = []
            for column in columns:
                filters.append(_parse_search_keyword('{0}:.*(?i){1}.*'.format(column, key), ':',
                                                     replace_expression='regex'))
            query += '(' + ' or '.join(filters) + ')'

    return query


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


def get_check_permission_client(authorization: str = Header(None)):
    """FastAPI dependency for getting client for checking permission.

    Reference:
        - https://fastapi.tiangolo.com/tutorial/dependencies/

    """
    if strtobool(os.environ.get('API_IGNORE_PERMISSION_CHECK', 'false')):
        return DummyCheckPermissionClient(authorization)
    return CheckPermissionClient(authorization)


def check_db_output_schema(dict: dict) -> None:
    """Check specified value is list
    """
    assert ('data' in dict.keys() and isinstance(dict['data'], list)), \
        'unexpected data was returned by PyDTK'

# TODO: Add client to override FastAPI dependency for running tests
# Reference: https://fastapi.tiangolo.com/advanced/testing-dependencies/
