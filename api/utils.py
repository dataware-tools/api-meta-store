#!/usr/bin/env python
# Copyright API authors
"""Utilities."""

import os

from api.exceptions import InvalidData


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
    key = f'"{key}"'
    value = ''.join(keyword.split(expression)[1:])
    value = value.replace('\\', '')
    if not value.isdecimal():
        value = f'"{value}"'
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


def validate_input_data(data: dict):
    """Validae the input data.

    Args:
        data (dict): input data

    """
    for k in data.keys():
        if k.startswith('_'):
            raise InvalidData('keys cannot start with "_"')


def filter_data(data: dict) -> dict:
    """Filter data for response message.

    Args:
        data (dict): original data

    Returns:
        (dict): data with filtered keys

    """
    return {k: v for k, v in data.items() if not k.startswith('_')}
