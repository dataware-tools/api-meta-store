#!/usr/bin/env python
# Copyright API authors
"""Database related functions."""
import math
from re import error as REError
from typing import List, Optional

from dataware_tools_api_helper.helpers import escape_string
from dataware_tools_api_helper.permissions import CheckPermissionClient
from fastapi import APIRouter, HTTPException, Body, Query, Depends

from api.exceptions import \
    ObjectExists, \
    ObjectDoesNotExist, \
    InvalidObject, \
    InvalidSortKey, \
    InvalidData
from api.utils import \
    check_db_output_schema, \
    parse_search_keyword, \
    filter_data, \
    validate_input_data, \
    validate_sort_key, \
    get_db_handler, \
    get_check_permission_client

router = APIRouter(
    tags=["databases"],
    responses={404: {"description": "Not found"}},
)


@router.get('/databases')
def list_databases(
    sort_key: str = 'database_id',
    per_page: int = 50,
    page: int = 1,
    search: str = None,
    search_key: Optional[List[str]] = Query(None),
    check_permission_client: CheckPermissionClient = Depends(get_check_permission_client)
):
    """List databases.

    Args:
        sort_key (str): Sort key
        per_page (int): Number of items to list in a page
        page (int): Current page
        search (str): Search keyword
        search_key (list): Which key to fuzzy search with
        check_permission_client (CheckPermissionClient): client for check permission

    Returns:
        (json): list of databases

    """
    try:
        resp = _list_databases(
            escape_string(sort_key, kind='key'),
            int(per_page),
            int(page),
            escape_string(search, kind='filtering'),
            search_key if search_key is None
            else [escape_string(key, kind='key') for key in search_key],
            check_permission_client
        )
        check_db_output_schema(resp)
        resp['data'] = [filter_data(item) for item in resp['data']]
        return resp
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except (AssertionError, InvalidSortKey, REError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except SyntaxError:
        pass


@router.post('/databases')
def create_database(
    check_permission_client: CheckPermissionClient = Depends(get_check_permission_client),
    data=Body(...),
):
    """Register new database information.

    Args:
        check_permission_client (CheckPermissionClient): client for check permission
        data (Body): metadata to register

    Returns:
        (json): detail of the created database

    """
    try:
        if 'database_id' in data.keys():
            data['database_id'] = escape_string(data['database_id'], kind='id')
        validate_input_data(data)
        check_permission_client.check_permissions('databases:write:add', data['database_id'])
        resp = _create_database(data)
        resp = filter_data(resp)
        return resp
    except ():  # FIXME: Specify exceptions corresponding to this error
        raise HTTPException(status_code=403, detail='Could not fetch data from database server')
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except (AssertionError, ObjectExists, InvalidData, InvalidObject) as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get('/databases/{database_id}')
def get_database(
    database_id: str,
    check_permission_client: CheckPermissionClient = Depends(get_check_permission_client),
):
    """Get database information.

    Args:
        database_id (str): database-id
        check_permission_client (CheckPermissionClient): client for check permission

    Returns:
        (json): detail of the database

    """
    try:
        database_id = escape_string(database_id, kind='id')
        check_permission_client.check_permissions('databases:read', database_id)
        resp = _get_database(database_id)
        resp = filter_data(resp)
        return resp
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail=f'No such database: {database_id}')
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


@router.patch('/databases/{database_id}')
def update_database(
    database_id: str,
    check_permission_client: CheckPermissionClient = Depends(get_check_permission_client),
    data=Body(...),
):
    """Patch database information.

    Args:
        database_id (str): database-id
        check_permission_client (CheckPermissionClient): client for check permission
        data (Body): database information

    Returns:
        (json): detail of the database

    """
    try:
        database_id = escape_string(database_id, kind='id')
        if 'database_id' in data.keys():
            data['database_id'] = escape_string(data['database_id'], kind='id')
        validate_input_data(data)
        check_permission_client.check_permissions('databases:write:update', database_id)
        resp = _update_database(database_id, data)
        resp = filter_data(resp)
        return resp
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail=f'No such database: {database_id}')
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except (AssertionError, InvalidData, InvalidObject) as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete('/databases/{database_id}')
def delete_database(
    database_id,
    check_permission_client: CheckPermissionClient = Depends(get_check_permission_client),
):
    """Delete database information.

    Args:
        database_id (str): database-id
        check_permission_client (CheckPermissionClient): client for check permission

    """
    try:
        database_id = escape_string(database_id, kind='id')
        check_permission_client.check_permissions('databases:write:delete', database_id)
        resp = _delete_database(database_id)
        return resp
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail='No such database')
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


def _list_databases(sort_key: str,
                    per_page: int,
                    page: int,
                    search_keyword: str,
                    search_key: list,
                    check_permission_client: CheckPermissionClient,
                    ):
    """Return a list of databases.

    Args:
        sort_key (str): Sort key
        per_page (int): Number of items per a page
        page (int): Index of current page
        search_keyword (str): Keyword
        search_key (list): Which key to fuzzy search with
        check_permission_client (CheckPermissionClient): client for check permission

    Returns:
        (dict): list of databases

    """
    begin = per_page * (page - 1)
    if search_key is None:
        search_key = ['database_id']

    # Prepare DBHandler
    handler = get_db_handler('database')

    # Validation
    validate_sort_key(sort_key, handler)

    # Prepare search query
    pql = parse_search_keyword(search_keyword, search_key)
    order_by = [(sort_key, 1)]

    # Read all data
    handler.read(pql=pql, limit=None, offset=0, order_by=order_by)
    all_data = handler.data

    # Filter database-ids based on user's permissions
    _, permitted_database_indices = check_permission_client.filter_permitted_databases(
        [item['database_id'] for item in all_data],
    )
    permitted_database_objects = [all_data[index] for index in permitted_database_indices]

    # Pagination
    paged_data = permitted_database_objects[begin:begin + per_page]
    total = len(permitted_database_objects)
    number_of_pages = math.ceil(total / per_page)

    resp = {
        'data': paged_data,
        'page': page,
        'per_page': per_page,
        'number_of_pages': number_of_pages,
        'sort_key': sort_key,
        'length': len(paged_data),
        'total': total
    }

    return resp


def _create_database(info: dict):
    """Register new database information.

    Args:
        info (dict): information of the database

    Returns:
        (dict): database info

    """
    # Check
    assert 'database_id' in info.keys(), '"database_id" must be specified'

    # Check if the 'database_id' already exist
    try:
        _ = _get_database(info['database_id'])
        raise ObjectExists('database_id "{}" already exists'.format(info['database_id']))
    except ObjectDoesNotExist:
        pass

    # Prepare database
    handler = get_db_handler('record', database_id=info['database_id'])
    handler.save()

    # Update database
    resp = _update_database(info['database_id'], info)

    return resp


def _get_database(database_id):
    """Get database information.

    Args:
        database_id (str): ID of database

    Returns:
        (dict): database information

    """
    # Prepare DBHandler
    handler = get_db_handler('database')

    # Execute query and read DB
    handler.read(pql=f'database_id == "{database_id}"')

    # Check
    if len(handler) == 0:
        raise ObjectDoesNotExist(f'No such database: {database_id}')
    if len(handler) > 1:
        raise InvalidObject('Multiple datbases found')

    # Return
    resp = next(handler)

    return resp


def _update_database(database_id, info):
    """Update a specific database.

    Args:
        database_id (str): ID of database
        info (dict): database info

    Returns:
        (dict): database information

    """
    # Check the existence of the database-id
    database_info = _get_database(database_id)

    # Check info
    if 'database_id' in info.keys():
        assert database_id == info['database_id'], "'database_id' cannot be changed"

    # Patch info
    database_info.update(info)

    # Save to DB
    handler = get_db_handler('database')
    handler.add_data(database_info)
    handler.save()

    # Return
    resp = database_info

    return resp


def _delete_database(database_id):
    """Delete the specified database.

    Args:
        database_id (str): ID of database

    """
    # Check the existence of the database-id
    info = _get_database(database_id)

    # Save to DB
    handler = get_db_handler('database')
    handler.remove_data(info)
    handler.save()

    # TODO: Remove the corresponding table for metadata

    resp = {
        'database_id': database_id
    }

    return resp
