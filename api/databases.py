#!/usr/bin/env python
# Copyright API authors
"""Database related functions."""

from fastapi import APIRouter, HTTPException, Body

from api.exceptions import ObjectExists, ObjectDoesNotExist, InvalidObject
from api.utils import parse_search_keyword, filter_data, validate_input_data, get_db_handler

router = APIRouter(
    prefix="/databases",
    tags=["databases"],
    responses={404: {"description": "Not found"}},
)


@router.get('')
def list_databases(
    sort_key: str = 'database_id',
    per_page: int = 50,
    page: int = 1,
    search: str = None
):
    """List databases.

    Args:
        sort_key (str): Sort key
        per_page (int): Number of items to list in a page
        page (int): Current page
        search (str): Search keyword

    Returns:
        (json): list of databases

    """
    try:
        resp = _list_databases(
            sort_key,
            per_page,
            page,
            search
        )
        assert 'data' in resp.keys()
        assert isinstance(resp['data'], list)
        resp['data'] = [filter_data(item) for item in resp['data']]
        return resp
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post('')
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
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get('/{database_id}')
def get_database(database_id: str):
    """Get database information.

    Args:
        database_id (str): database-id

    Returns:
        (json): detail of the database

    """
    try:
        resp = _get_database(database_id)
        resp = filter_data(resp)
        return resp
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail='No such database')


@router.patch('/{database_id}')
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
        resp = _update_database(database_id, data)
        resp = filter_data(resp)
        return resp
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail='No such database')
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete('/{database_id}')
def delete_database(database_id):
    """Delete database information.

    Args:
        database_id (str): database-id

    """
    try:
        _delete_database(database_id)
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail='No such database')


def _list_databases(sort_key: str,
                    per_page: int,
                    page: int,
                    search_keyword: str):
    """Return a list of databases.

    Args:
        sort_key (str): Sort key
        per_page (int): Number of items per a page
        page (int): Index of current page
        search_keyword (str): Keyword

    Returns:
        (dict): list of databases

    """
    begin = per_page * (page - 1)

    # Prepare DBHandler
    handler = get_db_handler('database')

    # Prepare search query
    pql = parse_search_keyword(search_keyword, ['database_id'])
    order_by = [(sort_key, 1)]

    # Read
    handler.read(pql=pql, limit=per_page, offset=begin, order_by=order_by)

    count = handler.count_total
    number_of_pages = count // per_page + 1
    data = handler.data

    resp = {
        'count': count,
        'data': data,
        'page': page,
        'per_page': per_page,
        'number_of_pages': number_of_pages,
        'sort_key': sort_key,
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
    assert 'database_id' in info.keys()

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
        raise ObjectDoesNotExist('No object found')
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

    return
