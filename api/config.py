#!/usr/bin/env python
# Copyright API authors
"""Record related functions."""

from fastapi import APIRouter, HTTPException, Body, Depends

from api.exceptions import ObjectDoesNotExist, InvalidObject, InvalidData
from api.databases import _get_database
from api.utils import \
    filter_data, \
    validate_input_data, \
    get_db_handler, \
    escape_string, \
    get_check_permission_client, \
    CheckPermissionClient

router = APIRouter(
    tags=["config"],
    responses={404: {"description": "Not found"}},
)


@router.get('/databases/{database_id}/config')
def get_config(
    database_id: str,
    check_permission_client: CheckPermissionClient = Depends(get_check_permission_client)
):
    """Get config.

    Args:
        database_id (str): database-id
        check_permission_client (CheckPermissionClient): client for check permission

    Returns:
        (json): config

    """
    try:
        check_permission_client.check_permissions('databases:read', database_id)
        resp = _get_config(escape_string(database_id, kind='id'))
        resp = filter_data(resp)
        return resp
    except AssertionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ObjectDoesNotExist as e:
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidObject as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch('/databases/{database_id}/config')
def update_config(
    database_id: str,
    check_permission_client: CheckPermissionClient = Depends(get_check_permission_client),
    data=Body(...),
):
    """Patch config.

    Args:
        database_id (str): database-id
        check_permission_client (CheckPermissionClient): client for check permission
        data (Body): new config

    Returns:
        (json): config

    """
    try:
        validate_input_data(data)
        check_permission_client.check_permissions('databases:write:update', database_id)
        resp = _update_config(escape_string(database_id, kind='id'), data)
        resp = filter_data(resp)
        return resp
    except (AssertionError, InvalidData) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ObjectDoesNotExist as e:
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidObject as e:
        raise HTTPException(status_code=500, detail=str(e))


def _get_config(database_id: str):
    """Get config.

    Args:
        database_id (str): ID of the database

    Returns:
        (dict): config

    """
    # Check the existence of the database-id
    _ = _get_database(database_id)

    # Prepare handler
    handler = get_db_handler('config', database_id=database_id)
    resp = handler.config
    return resp


def _update_config(database_id: str, config):
    """Update config.

    Args:
        database_id (str): ID of the database
        config (dict): record info

    Returns:
        (dict): config

    """
    # Check the existence of the database-id
    _ = _get_database(database_id)

    # Prepare DBHandler
    handler = get_db_handler('config', database_id=database_id)

    # Validate config
    if 'columns' in config.keys():
        # Make sure that column names are the same
        column_names_original = set([c['name'] for c in handler.config['columns']])
        column_names_new = set([c['name'] for c in config['columns']])
        assert column_names_original.issubset(column_names_new), \
            'You can neither remove columns nor change the names'

        # Check contents in each columns
        for column in config['columns']:
            assert 'name' in column.keys(), 'config for columns should have "name" key'
            assert 'dtype' in column.keys(), 'config for columns should have "dtype" key'
            assert 'aggregation' in column.keys(), \
                'config for columns should have "aggregation" key'
            # assert column['dtype'] in DTYPE_MAP.keys()    # This is too strict
            # TODO: Check the value of column['aggregation']

    if 'index_columns' in config.keys():
        assert len(config['index_columns']) > 0, \
            'At least one column must be selected as index_column'

    # Update config
    handler.config.update(config)

    # Save
    handler.save()

    # Return
    resp = handler.config

    return resp
