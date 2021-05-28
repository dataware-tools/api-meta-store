#!/usr/bin/env python
# Copyright API authors
"""Record related functions."""

from fastapi import APIRouter, HTTPException, Body

from api.exceptions import ObjectExists, ObjectDoesNotExist, InvalidObject, InvalidData
from api.databases import _get_database
from api.utils import parse_search_keyword, filter_data, validate_input_data, get_db_handler

router = APIRouter(
    tags=["record"],
    responses={404: {"description": "Not found"}},
)


@router.get('/databases/{database_id}/records')
def list_records(
    database_id: str,
    *,
    sort_key: str = 'record_id',
    per_page: int = 50,
    page: int = 1,
    search: str = None
):
    """List records.

    Args:
        database_id (str): Database-id
        sort_key (str): Sort key
        per_page (int): Number of items to list in a page
        page (int): Current page
        search (str): Search keyword

    Returns:
        (json): list of records

    """
    try:
        resp = _list_records(
            database_id,
            sort_key,
            per_page,
            page,
            search
        )
        assert 'data' in resp.keys()
        assert isinstance(resp['data'], list)
        resp['data'] = [filter_data(item) for item in resp['data']]
        return resp
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail='No such database')
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post('/databases/{database_id}/records')
def create_record(database_id: str, *, data=Body(...)):
    """Register new record information.

    Args:
        database_id (str): database-id
        data (Body): metadata to register

    Returns:
        (json): detail of the created record

    """
    try:
        validate_input_data(data)
        resp = _create_record(database_id, data)
        resp = filter_data(resp)
        return resp
    except ():  # FIXME: Specify exceptions corresponding to this error
        raise HTTPException(status_code=403, detail='Could not fetch data from database server')
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ObjectDoesNotExist as e:
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidData as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get('/databases/{database_id}/records/{record_id}')
def get_record(database_id: str, record_id: str):
    """Get record information.

    Args:
        database_id (str): database-id
        record_id (str): record-id

    Returns:
        (json): detail of the record

    """
    try:
        resp = _get_record(database_id, record_id)
        resp = filter_data(resp)
        return resp
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ObjectDoesNotExist as e:
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidObject as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch('/databases/{database_id}/records/{record_id}')
def update_record(database_id: str, record_id: str, data=Body(...)):
    """Patch record information.

    Args:
        database_id (str): database-id
        record_id (str): record-id
        data (Body): record information

    Returns:
        (json): detail of the record

    """
    try:
        validate_input_data(data)
        resp = _update_record(database_id, record_id, data)
        resp = filter_data(resp)
        return resp
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail='No such record')
    except InvalidObject as e:
        raise HTTPException(status_code=500, detail=str(e))
    except InvalidData as e:
        raise HTTPException(status_code=400, detail=str(e))
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete('/databases/{database_id}/records/{record_id}')
def delete_record(database_id: str, record_id: str):
    """Delete record information.

    Args:
        database_id (str): database-id
        record_id (str): record-id

    """
    try:
        resp = _delete_record(database_id, record_id)
        return resp
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail='No such database')


def _list_records(database_id: str,
                  sort_key: str,
                  per_page: int,
                  page: int,
                  search_keyword: str):
    """Return a list of records.

    Args:
        database_id (str): database-id
        sort_key (str): Sort key
        per_page (int): Number of items per a page
        page (int): Index of current page
        search_keyword (str): Keyword

    Returns:
        (dict): list of records

    """
    begin = per_page * (page - 1)

    # Check if the database exist
    _ = _get_database(database_id)

    # Prepare DBHandler
    handler = get_db_handler('record', database_id=database_id)

    # Prepare search query
    pql = parse_search_keyword(search_keyword, ['record_id'])
    order_by = [(sort_key, 1)]

    # Read
    handler.read(pql=pql, limit=per_page, offset=begin, order_by=order_by, group_by='record_id')

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
        'total': total,
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
    assert database_id is not None
    assert 'record_id' in info.keys()

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


def _get_record(database_id: str, record_id: str):
    """Get record information.

    Args:
        database_id (str): ID of the database
        record_id (str): ID of the record

    Returns:
        (dict): record information

    """
    # Prepare DBHandler
    handler = get_db_handler('record', database_id=database_id)

    # Execute query and read DB
    handler.read(pql=f'record_id == "{record_id}"', group_by='record_id')

    # Check
    if len(handler) == 0:
        raise ObjectDoesNotExist('No object found')
    if len(handler) > 1:
        raise InvalidObject('Multiple objects found')

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
    _ = _get_record(database_id, record_id)

    # Prepare DBHandler
    handler = get_db_handler('record', database_id=database_id)

    # Execute query and read DB
    handler.read(pql=f'record_id == "{record_id}"')

    # List-up keys whose values are different from those on DB
    keys_to_update = set()
    for data in handler:
        for key in info.keys():
            if key in data.keys() and data[key] != info[key]:
                keys_to_update.add(key)

    # Check keys
    for key in keys_to_update:
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
    resp = info

    return resp


def _delete_record(database_id: str, record_id: str):
    """Delete the specified database.

    Args:
        database_id (str): ID of the database
        record_id (str): ID of the record

    """
    # Check the existence of the database-id
    info = _get_record(database_id, record_id)

    # Save to DB
    handler = get_db_handler('record', database_id=database_id)
    handler.remove_data(info)
    handler.save()

    resp = {
        'database_id': database_id,
        'record_id': record_id
    }

    return resp
