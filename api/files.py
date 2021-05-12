#!/usr/bin/env python
# Copyright API authors
"""File related functions."""

from fastapi import APIRouter, HTTPException, Body

from api.exceptions import ObjectExists, ObjectDoesNotExist, InvalidObject, InvalidData
from api.databases import _get_database
from api.utils import parse_search_keyword, filter_data, validate_input_data, get_db_handler

router = APIRouter(
    prefix="/files",
    tags=["file"],
    responses={404: {"description": "Not found"}},
)


@router.get('')
def list_files(
    database_id: str,
    record_id: str,
    sort_key: str = 'path',
    per_page: int = 50,
    page: int = 1,
    search: str = None
):
    """List files.

    Args:
        database_id (str): Database ID
        record_id (str): Record ID
        sort_key (str): Sort key
        per_page (int): Number of items to list in a page
        page (int): Current page
        search (str): Search keyword

    Returns:
        (json): list of files

    """
    try:
        resp = _list_files(
            database_id,
            record_id,
            sort_key,
            per_page,
            page,
            search
        )
        assert 'data' in resp.keys()
        assert isinstance(resp['data'], list)
        resp['data'] = [filter_data(item) for item in resp['data']]
        return resp
    except ObjectDoesNotExist as e:
        raise HTTPException(status_code=404, detail=str(e))
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post('')
def create_file(database_id: str, record_id: str, data=Body(...)):
    """Register new file information.

    Args:
        database_id (str): database-id
        record_id (str): record-id
        data (Body): metadata to register

    Returns:
        (json): file information

    """
    try:
        validate_input_data(data)
        resp = _create_file(database_id, record_id, data)
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


@router.get('/{path:path}')
def get_file(path: str, record_id: str, database_id: str):
    """Get file information.

    Args:
        path (str): file-path
        record_id (str): record-id
        database_id (str): database-id

    Returns:
        (json): detail of the file

    """
    try:
        resp = _get_file(database_id, record_id, path)
        resp = filter_data(resp)
        return resp
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ObjectDoesNotExist as e:
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidObject as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch('/{path:path}')
def update_file(path: str, record_id: str, database_id: str, data=Body(...)):
    """Update file information.

    Args:
        path (str): file path
        record_id (str): record-id
        database_id (str): database-id
        data (Body): file information

    Returns:
        (json): detail of the file

    """
    try:
        validate_input_data(data)
        resp = _update_file(database_id, record_id, path, data)
        resp = filter_data(resp)
        return resp
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail='No such file')
    except InvalidObject as e:
        raise HTTPException(status_code=500, detail=str(e))
    except InvalidData as e:
        raise HTTPException(status_code=400, detail=str(e))
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete('/{path:path}')
def delete_file(path: str, record_id: str, database_id: str):
    """Delete file information.

    Args:
        path (str): file path
        record_id (str): record-id
        database_id (str): database-id

    """
    try:
        _delete_file(database_id, record_id, path)
    except ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail='No such database')


def _list_files(database_id: str,
                record_id: str,
                sort_key: str,
                per_page: int,
                page: int,
                search_keyword: str):
    """Return a list of files.

    Args:
        database_id (str): database-id
        record_id (str): record-id
        sort_key (str): Sort key
        per_page (int): Number of items per a page
        page (int): Index of current page
        search_keyword (str): Keyword

    Returns:
        (dict): list of files

    """
    begin = per_page * (page - 1)

    # Check if the database exists
    _ = _get_database(database_id)

    # Prepare DBHandler
    handler = get_db_handler('file', database_id=database_id)

    # Prepare search query
    pql = parse_search_keyword(search_keyword, ['path'])
    if pql is None:
        pql = f'record_id == "{record_id}"'
    else:
        pql = f'record_id == "{record_id}" and {pql}'
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


def _create_file(database_id: str, record_id: str, info: dict):
    """Register new file information.

    Args:
        database_id (str): database-id
        record_id (str): record-id
        info (dict): information of the file

    Returns:
        (dict): file info

    """
    # Check
    assert database_id is not None
    assert record_id is not None
    assert 'path' in info.keys()

    # Check if the 'database_id' already exist
    try:
        _ = _get_database(database_id)
    except ObjectDoesNotExist:
        raise ObjectDoesNotExist(f'database "{database_id}" does not exist')

    # Prepare metadata
    info['record_id'] = record_id

    # Prepare DBHandler
    handler = get_db_handler('file', database_id=database_id)
    handler.add_data(info)
    handler.save()

    resp = info
    return resp


def _get_file(database_id: str, record_id: str, path: str):
    """Get file information.

    Args:
        database_id (str): ID of the database
        record_id (str): ID of the record
        path (str): Path of the file

    Returns:
        (dict): file information

    """
    # Prepare DBHandler
    handler = get_db_handler('file', database_id=database_id)

    # TODO: Substring base-dir from the given path

    # Execute query and read DB
    handler.read(pql=f'record_id == "{record_id}" and path == "{path}"')

    # Check
    if len(handler) == 0:
        raise ObjectDoesNotExist('No object found')
    if len(handler) > 1:
        raise InvalidObject('Multiple objects found')

    # Return
    resp = handler.data[0]

    return resp


def _update_file(database_id: str, record_id: str, path: str, info):
    """Update specific file information.

    Args:
        database_id (str): ID of the database
        record_id (str): ID of the record
        path (str): Path to the file
        info (dict): file info

    Returns:
        (dict): file information

    """
    # Check the existence of the file
    _ = _get_file(database_id, record_id, path)

    # Prepare DBHandler
    handler = get_db_handler('file', database_id=database_id)

    # TODO: Substring base-dir from the given path

    # Execute query and read DB
    handler.read(pql=f'record_id == "{record_id}" and path == "{path}"')

    # List-up keys whose values are different from those on DB
    keys_to_update = set()
    for data in handler:
        for key in info.keys():
            if key in data.keys() and data[key] != info[key]:
                keys_to_update.add(key)

    # Check keys
    for key in keys_to_update:
        if key in handler.config['index_columns']:
            raise InvalidData(f'Key "{key}" cannot be updated. Please delete this entry')

    # Update data one-by-one
    for data in handler.data:
        data.update(info)
        handler.add_data(data, strategy='overwrite')

    # Save
    handler.save()

    # Return
    resp = info

    return resp


def _delete_file(database_id: str, record_id: str, path: str):
    """Delete the specified file.

    Args:
        database_id (str): ID of the database
        record_id (str): ID of the record
        path (str): File path

    """
    # Check the existence of the file
    info = _get_file(database_id, record_id, path)

    # Save to DB
    handler = get_db_handler('file', database_id=database_id)
    handler.remove_data(info)
    handler.save()

    return
