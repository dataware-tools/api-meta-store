#!/usr/bin/env python
# Copyright API authors
"""Test code."""

from hypothesis import settings
import schemathesis
from schemathesis import Case
from schemathesis.stateful import StepResult, Direction

from api.server import api
from .common import _init_database, _set_env
from .test_databases import add_database, remove_database
from .test_records import add_record, remove_record


# Prepare schema
with open('./protocols/schemas/apis/{}/schema.v1.yaml'.format(api.title), 'r') as f:
    schema = schemathesis.from_file(
        f,
        app=api,
        # operation_id='createRecord',
        endpoint='^(/databases/{database_id}/records)'
    )
# schema.add_link(
#     source=schema["/databases"]["POST"],
#     target=schema["/databases/{database_id}"]["GET"],
#     status_code="200",
#     parameters={"database_id": "$response.body#/database_id"},
# )
schema.add_link(
    source=schema["/databases/{database_id}/records"]["POST"],
    target=schema["/databases/{database_id}/records/{record_id}"]["GET"],
    status_code="200",
    parameters={"record_id": "$response.body#/record_id"},
)


# Define tests
class APIWorkflow(schema.as_state_machine()):
    """API Workflow."""

    def transform(self, result: StepResult, direction: Direction, case: Case) -> Case:
        pass

    def setup(self):
        _set_env()
        _init_database()

    def teardown(self):
        pass

    def before_call(self, case):
        if case is None:
            return
        if not hasattr(case, 'path'):
            return
        if '{database_id}' in case.path:
            add_database(case.path_parameters['database_id'])
            if '{record_id}' in case.path:
                if case.method in ['GET', 'PATCH', 'DELETE']:
                    add_record(
                        database_id=case.path_parameters['database_id'],
                        record_id=case.path_parameters['record_id']
                    )
                if case.method in ['CREATE']:
                    remove_record(
                        database_id=case.path_parameters['database_id'],
                        record_id=case.path_parameters['record_id']
                    )
            else:
                if case.method in ['GET', 'PATCH', 'DELETE']:
                    pass
                if case.method in ['CREATE']:
                    remove_database(case.path_parameters['database_id'])

    def after_call(self, response, case):
        print(f'[{response.status_code}] {case.method} {case.formatted_path} {case}')

    @staticmethod
    def check_condition(response, case):
        # Run this check only for `GET /databases`
        if case.method == 'GET' and case.path == '/databases':
            value = response.json()
            if response.status_code == 400:
                assert 'detail' in value.keys()

    def validate_response(self, response, case, **kwargs):
        # Run all default checks together with the new one
        super().validate_response(response, case, additional_checks=(self.check_condition,))


# Run tests
TestCase = APIWorkflow.TestCase
TestCase.settings = settings(max_examples=10, stateful_step_count=5, deadline=None)


