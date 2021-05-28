#!/usr/bin/env python
# Copyright API authors
"""File related functions."""

from fastapi import APIRouter, HTTPException, Body

from api.exceptions import ObjectExists, ObjectDoesNotExist, InvalidObject, InvalidData
from api.databases import _get_database
from api.utils import parse_search_keyword, filter_data, validate_input_data, get_db_handler

router = APIRouter(
    tags=["file"],
    responses={404: {"description": "Not found"}},
)


@router.get('/databases/{database_id}/files')
def list_files(
    database_id: str,
    *,
    record_id: str = None,
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
        resp['data'] = [_expose_uuid(item) for item in resp['data']]
        resp['data'] = [filter_data(item) for item in resp['data']]
        return resp
    except ObjectDoesNotExist as e:
        raise HTTPException(status_code=404, detail=str(e))
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post('/databases/{database_id}/files')
def create_file(
    database_id: str,
    *,
    data=Body(...)
):
    """Register new file information.

    Args:
        database_id (str): database-id
        data (Body): metadata to register

    Returns:
        (json): file information

    """
    try:
        validate_input_data(data)
        resp = _create_file(database_id, data)
        resp = _expose_uuid(resp)
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


@router.get('/databases/{database_id}/files/{uuid}')
def get_file(database_id: str, uuid: str):
    """Get file information.

    Args:
        database_id (str): database-id
        uuid (str): unique-id

    Returns:
        (json): detail of the file

    """
    try:
        resp = _get_file(database_id, uuid)
        resp = _expose_uuid(resp)
        resp = filter_data(resp)
        return resp
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ObjectDoesNotExist as e:
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidObject as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch('/databases/{database_id}/files/{uuid}')
def update_file(
    database_id: str,
    uuid: str,
    *,
    data=Body(...)
):
    """Update file information.

    Args:
        database_id (str): database-id
        uuid (str): unique-id
        data (Body): file information

    Returns:
        (json): detail of the file

    """
    try:
        validate_input_data(data)
        resp = _update_file(database_id, uuid, data)
        resp = _expose_uuid(resp)
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


@router.delete('/databases/{database_id}/files/{uuid}')
def delete_file(database_id: str, uuid: str):
    """Delete file information.

    Args:
        database_id (str): database-id
        uuid (str): unique-id

    """
    try:
        resp = _delete_file(database_id, uuid)
        return resp
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
    if record_id is None:
        record_id = 'regex(".*")'
    else:
        record_id = f'"{record_id}"'

    # Check if the database exists
    _ = _get_database(database_id)

    # Prepare DBHandler
    handler = get_db_handler('file', database_id=database_id)

    # Prepare search query
    pql = parse_search_keyword(search_keyword, ['path'])
    if pql is None:
        pql = f'record_id == {record_id}'
    else:
        pql = f'record_id == {record_id} and {pql}'
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
        'total': total,
    }

    return resp


def _create_file(database_id: str, info: dict):
    """Register new file information.

    Args:
        database_id (str): database-id
        info (dict): information of the file

    Returns:
        (dict): file info

    """
    # Check
    assert database_id is not None
    assert 'path' in info.keys()

    # Check if the 'database_id' already exist
    try:
        _ = _get_database(database_id)
    except ObjectDoesNotExist:
        raise ObjectDoesNotExist(f'database "{database_id}" does not exist')

    # Prepare DBHandler
    handler = get_db_handler('file', database_id=database_id)
    handler.add_data(info)
    handler.save()

    resp = handler.data[0]
    return resp


def _get_file(database_id: str, uuid: str):
    """Get file information.

    Args:
        database_id (str): ID of the database
        uuid (str): Unique-id of the file

    Returns:
        (dict): file information

    """
    # Prepare DBHandler
    handler = get_db_handler('file', database_id=database_id)

    # Execute query and read DB
    handler.read(pql=f'_uuid == "{uuid}"')

    # Check
    if len(handler) == 0:
        raise ObjectDoesNotExist('No object found')
    if len(handler) > 1:
        raise InvalidObject('Multiple objects found')

    # Return
    resp = handler.data[0]

    return resp


def _update_file(database_id: str, uuid: str, info):
    """Update specific file information.

    Args:
        database_id (str): ID of the database
        uuid (str): Unique-id of the file
        info (dict): file info

    Returns:
        (dict): file information

    """
    # Check the existence of the file
    _ = _get_file(database_id, uuid)

    # Prepare DBHandler
    handler = get_db_handler('file', database_id=database_id)

    # Execute query and read DB
    handler.read(pql=f'_uuid == "{uuid}"')

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


def _delete_file(database_id: str, uuid: str):
    """Delete the specified file.

    Args:
        database_id (str): ID of the database
        uuid (str): Unique-id of the file

    """
    # Check the existence of the file
    info = _get_file(database_id, uuid)

    # Save to DB
    handler = get_db_handler('file', database_id=database_id)
    handler.remove_data(info)
    handler.save()

    resp = {
        'database_id': database_id,
        'uuid': uuid
    }

    return resp


def _expose_uuid(data: dict):
    """Expose _uuid as uuid.

    Args:
        data (dict): metadata

    Returns:
        (dict): data where uuids are exposed

    """
    if not isinstance(data, dict):
        return data
    if '_uuid' in data.keys() and 'uuid' not in data.keys():
        data['uuid'] = data['_uuid']

    return data
