#!/usr/bin/env python
# Copyright API authors
"""Database related functions."""

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Body, Query

from api.exceptions import \
    ObjectExists, \
    ObjectDoesNotExist, \
    InvalidObject, \
    InvalidSortKey, \
    InvalidData
from api.utils import \
    parse_search_keyword, \
    filter_data, \
    validate_input_data, \
    validate_sort_key, \
    escape_string, \
    get_db_handler

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
    search_key: Optional[List[str]] = Query(None)
):
    """List databases.

    Args:
        sort_key (str): Sort key
        per_page (int): Number of items to list in a page
        page (int): Current page
        search (str): Search keyword
        search_key (list): Which key to fuzzy search with

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
            else [escape_string(key, kind='key') for key in search_key]
        )
        assert 'data' in resp.keys()
        assert isinstance(resp['data'], list)
        resp['data'] = [filter_data(item) for item in resp['data']]
        return resp
    except (AssertionError, InvalidSortKey) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except SyntaxError:
        pass


@router.post('/databases')
def create_database(data=Body(...)):
    """Register new database information.

    Args:
        data (Body): metadata to register

    Returns:
        (json): detail of the created database

    """
    try:
        validate_input_data(data)
        resp = _create_database(data)
        resp = filter_data(resp)
        return resp
    except ():  # FIXME: Specify exceptions corresponding to this error
        raise HTTPException(status_code=403, detail='Could not fetch data from database server')
    except (AssertionError, ObjectExists, InvalidData, InvalidObject) as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get('/databases/{database_id}')
def get_database(database_id: str):
    """Get database information.

    Args:
        database_id (str): database-id

    Returns:
        (json): detail of the database

    """
    try:
        resp = _get_database(escape_string(database_id, kind='id'))
        resp = filter_data(resp)
        return resp
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail=f'No such database: {database_id}')


@router.patch('/databases/{database_id}')
def update_database(database_id: str, data=Body(...)):
    """Patch database information.

    Args:
        database_id (str): database-id
        data (Body): database information

    Returns:
        (json): detail of the database

    """
    try:
        validate_input_data(data)
        resp = _update_database(escape_string(database_id, kind='id'), data)
        resp = filter_data(resp)
        return resp
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail=f'No such database: {database_id}')
    except (AssertionError, InvalidData, InvalidObject) as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete('/databases/{database_id}')
def delete_database(database_id):
    """Delete database information.

    Args:
        database_id (str): database-id

    """
    try:
        resp = _delete_database(escape_string(database_id, kind='id'))
        return resp
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail='No such database')


def _list_databases(sort_key: str,
                    per_page: int,
                    page: int,
                    search_keyword: str,
                    search_key: list):
    """Return a list of databases.

    Args:
        sort_key (str): Sort key
        per_page (int): Number of items per a page
        page (int): Index of current page
        search_keyword (str): Keyword
        search_key (list): Which key to fuzzy search with

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

    # Read
    handler.read(pql=pql, limit=per_page, offset=begin, order_by=order_by)

    total = handler.count_total
    number_of_pages = total // per_page + 1
    data = handler.data

    resp = {
        'data': data,
        'page': page,
        'per_page': per_page,
        'number_of_pages': number_of_pages,
        'sort_key': sort_key,
        'length': len(data),
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
        raise InvalidObject('Multiple objects found')

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
    assert 'database_id' in info.keys()
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
