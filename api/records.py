#!/usr/bin/env python
# Copyright API authors
"""Record related functions."""

import copy
import math
from re import error as REError
from typing import List, Optional

from dataware_tools_api_helper.helpers import escape_string
from dataware_tools_api_helper.permissions import CheckPermissionClient
from fastapi import APIRouter, Body, Depends, HTTPException, Query

from api.exceptions import ObjectDoesNotExist, InvalidObject, InvalidData, InvalidSortKey
from api.databases import _get_database
from api.utils import \
    check_db_output_schema, \
    parse_search_keyword, \
    filter_data, \
    validate_input_data, \
    validate_sort_key, \
    get_db_handler, \
    generate_record_id, \
    get_check_permission_client

router = APIRouter(
    tags=["record"],
    responses={404: {"description": "Not found"}},
)


@router.get('/databases/{database_id}/records')
def list_records(
    database_id: str,
    *,
    sort_key: str = 'record_id',
    sort_order: int = 1,
    per_page: int = 50,
    page: int = 1,
    search: str = None,
    search_key: Optional[List[str]] = Query(None),
    check_permission_client: CheckPermissionClient = Depends(get_check_permission_client),
):
    """List records.

    Args:
        database_id (str): Database-id
        sort_key (str): Sort key
        sort_order (str): Sort order (1: asc, -1: desc)
        per_page (int): Number of items to list in a page
        page (int): Current page
        search (str): Search keyword
        search_key (list): Which key to fuzzy search with
        check_permission_client (CheckPermissionClient): client for check permission

    Returns:
        (json): list of records

    """
    try:
        # Escape string
        database_id = escape_string(database_id, kind='id')

        # Get columns to filter based on permission
        columns_to_filter = check_permission_client.columns_to_filter(database_id)

        # Check permissions
        check_permission_client.check_permissions('metadata:read:public', database_id)

        resp = _list_records(
            escape_string(database_id, kind='id'),
            escape_string(sort_key, kind='key'),
            sort_order,
            per_page,
            page,
            escape_string(search, kind='filtering'),
            search_key if search_key is None
            else [escape_string(key, kind='key') for key in search_key]
        )
        check_db_output_schema(resp)
        resp['data'] = [filter_data(item, excludes=columns_to_filter) for item in resp['data']]
        return resp
    except ObjectDoesNotExist as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except (AssertionError, InvalidSortKey, REError) as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post('/databases/{database_id}/records')
def create_record(
    database_id: str, *,
    check_permission_client: CheckPermissionClient = Depends(get_check_permission_client),
    data=Body(...),
):
    """Register new record information.

    Args:
        database_id (str): database-id
        check_permission_client (CheckPermissionClient): client for check permission
        data (Body): metadata to register

    Returns:
        (json): detail of the created record

    """
    try:
        # Escape string
        database_id = escape_string(database_id, kind='id')

        # Get columns to filter based on permission
        columns_to_filter = check_permission_client.columns_to_filter(database_id)

        # Validate data
        validate_input_data(data)

        # Check permissions
        check_permission_client.check_permissions('metadata:write:add', database_id)

        resp = _create_record(escape_string(database_id, kind='id'), data)
        resp = filter_data(resp, excludes=columns_to_filter)
        return resp
    except ():  # FIXME: Specify exceptions corresponding to this error
        raise HTTPException(status_code=403, detail='Could not fetch data from database server')
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except (AssertionError, InvalidData, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ObjectDoesNotExist as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get('/databases/{database_id}/records/{record_id}')
def get_record(
    database_id: str, record_id: str,
    check_permission_client: CheckPermissionClient = Depends(get_check_permission_client),
):
    """Get record information.

    Args:
        database_id (str): database-id
        record_id (str): record-id
        check_permission_client (CheckPermissionClient): client for check permission

    Returns:
        (json): detail of the record

    """
    try:
        # Escape string
        database_id = escape_string(database_id, kind='id')

        # Get columns to filter based on permission
        columns_to_filter = check_permission_client.columns_to_filter(database_id)

        # Check permissions
        check_permission_client.check_permissions('metadata:read:public', database_id)

        resp = _get_record(
            escape_string(database_id, kind='id'),
            escape_string(record_id, kind='id')
        )
        resp = filter_data(resp, excludes=columns_to_filter)
        return resp
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ObjectDoesNotExist as e:
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidObject as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch('/databases/{database_id}/records/{record_id}')
def update_record(
    database_id: str, record_id: str,
    check_permission_client: CheckPermissionClient = Depends(get_check_permission_client),
    data=Body(...),
):
    """Patch record information.

    Args:
        database_id (str): database-id
        record_id (str): record-id
        check_permission_client (CheckPermissionClient): client for check permission
        data (Body): record information

    Returns:
        (json): detail of the record

    """
    try:
        # Escape string
        database_id = escape_string(database_id, kind='id')

        # Get columns to filter based on permission
        columns_to_filter = check_permission_client.columns_to_filter(database_id)

        # Validate data
        validate_input_data(data)

        # Check permissions
        check_permission_client.check_permissions('metadata:write:update', database_id)

        resp = _update_record(
            escape_string(database_id, kind='id'),
            escape_string(record_id, kind='id'),
            data
        )
        resp = filter_data(resp, excludes=columns_to_filter)
        return resp
    except (AssertionError, InvalidData, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ObjectDoesNotExist as e:
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidObject as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete('/databases/{database_id}/records/{record_id}')
def delete_record(
    database_id: str,
    record_id: str,
    check_permission_client: CheckPermissionClient = Depends(get_check_permission_client),
):
    """Delete record information.

    Args:
        database_id (str): database-id
        record_id (str): record-id
        check_permission_client (CheckPermissionClient): client for check permission

    """
    try:
        # Escape string
        database_id = escape_string(database_id, kind='id')

        # Check permissions
        check_permission_client.check_permissions('metadata:write:delete', database_id)

        resp = _delete_record(
            escape_string(database_id, kind='id'),
            escape_string(record_id, kind='id')
        )
        return resp
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail='No such database')


def _list_records(database_id: str,
                  sort_key: str,
                  sort_order: int,
                  per_page: int,
                  page: int,
                  search_keyword: str,
                  search_key: list):
    """Return a list of records.

    Args:
        database_id (str): database-id
        sort_key (str): Sort key
        sort_order (int): Sort order (1: asc, -1: desc)
        per_page (int): Number of items per a page
        page (int): Index of current page
        search_keyword (str): Keyword
        search_key (list): Which key to fuzzy search with

    Returns:
        (dict): list of records

    """
    begin = per_page * (page - 1)
    if search_key is None:
        search_key = ['record_id']

    # Check if the database exist
    _ = _get_database(database_id)

    # Prepare DBHandler
    handler = get_db_handler('record', database_id=database_id)

    # Validation
    validate_sort_key(sort_key, handler)

    # Prepare search query
    pql = parse_search_keyword(search_keyword, search_key)
    order_by = [(sort_key, sort_order)]

    # Read
    handler.read(pql=pql, limit=per_page, offset=begin, order_by=order_by, group_by='record_id')

    total = handler.count_total
    number_of_pages = math.ceil(total / per_page)
    data = handler.data

    resp = {
        'data': data,
        'page': page,
        'per_page': per_page,
        'number_of_pages': number_of_pages,
        'sort_key': sort_key,
        'sort_order': sort_order,
        'length': len(data),
        'total': total,
        'database_id': database_id
    }

    return resp


def _create_record(database_id: str, info: dict):
    """Register new record information.

    Args:
        database_id (str): database-id
        info (dict): information of the record

    Returns:
        (dict): record info

    """
    # Check
    assert database_id is not None, "Database ID is required"

    # Generate record_id if needed
    if 'record_id' not in info.keys():
        info['record_id'] = generate_record_id()

    # Check if the 'database_id' already exist
    try:
        _ = _get_database(database_id)
    except ObjectDoesNotExist:
        raise ObjectDoesNotExist(f'database "{database_id}" does not exist')

    # Prepare DBHandler
    handler = get_db_handler('record', database_id=database_id)
    handler.add_data(info)
    handler.save()

    resp = info
    return resp


def _get_record(database_id: str, record_id: str, strict_check=True):
    """Get record information.

    Args:
        database_id (str): ID of the database
        record_id (str): ID of the record
        strict_check (bool): Check that metadata is grouped by 'record_id'

    Returns:
        (dict): record information

    """
    # Prepare DBHandler
    handler = get_db_handler('record', database_id=database_id)

    # Execute query and read DB
    handler.read(pql=f'record_id == "{record_id}"', group_by='record_id')

    # Check
    if len(handler) == 0:
        raise ObjectDoesNotExist('No record found')
    if len(handler) > 1 and strict_check:
        raise InvalidObject('Multiple records found')

    # Return
    resp = handler.data[0]

    return resp


def _update_record(database_id: str, record_id: str, info):
    """Update a specific database.

    Args:
        database_id (str): ID of the database
        record_id (str): ID of the record
        info (dict): record info

    Returns:
        (dict): record information

    """
    # Check the existence of the record-id
    _ = _get_record(database_id, record_id, strict_check=False)

    # Prepare DBHandler
    handler = get_db_handler('record', database_id=database_id)

    # Execute query and read DB
    handler.read(pql=f'record_id == "{record_id}"')

    # List-up keys whose values are different from those on DB
    keys_to_add = set()
    keys_to_update = set()
    for data in handler:
        for key in info.keys():
            if key not in data.keys():
                keys_to_add.add(key)
            elif data[key] != info[key]:
                keys_to_update.add(key)

    # Check keys
    for key in {*keys_to_add, *keys_to_update}:
        if key in handler.config['index_columns']:
            raise InvalidData(f'Key "{key}" cannot be updated. Please replace this record.')
        if key not in [c['name'] for c in handler.config['columns']]:
            continue
        column_info = next(filter(lambda c: c['name'] == key, handler.config['columns']))
        if 'aggregation' not in column_info.keys():
            continue
        if column_info['aggregation'] in ['first']:
            continue
        raise InvalidData(f'Key "{key}" cannot be updated. Consider updating on file-level.')

    # Update data one-by-one
    for data in handler.data:
        data.update(info)
        handler.add_data(data, strategy='overwrite')

    # Save
    handler.save()

    # Return
    resp = _get_record(database_id=database_id, record_id=record_id)

    return resp


def _delete_record(database_id: str, record_id: str):
    """Delete the specified database.

    Args:
        database_id (str): ID of the database
        record_id (str): ID of the record

    """
    # Check the existence of the database-id
    _ = _get_record(database_id, record_id, strict_check=False)

    # Prepare DBHandler
    handler = get_db_handler('record', database_id=database_id)

    # Execute query and read DB
    handler.read(pql=f'record_id == "{record_id}"')

    # Copy metadata to delete
    data_to_delete = copy.deepcopy(handler.data)

    # Remove metadata
    for data in data_to_delete:
        handler.remove_data(data)
    handler.save()

    resp = {
        'database_id': database_id,
        'record_id': record_id
    }

    return resp
