#!/usr/bin/env python
# Copyright API authors
"""Exceptions."""


class ObjectExists(Exception):
    """Exception for the case the object already exist."""
    pass


class ObjectDoesNotExist(Exception):
    """Exception for the case no object exist."""
    pass


class InvalidObject(Exception):
    """Exception for the case the retrieved object is invalid."""
    pass


class InvalidData(Exception):
    """Exception for the case the input data is invalid."""
    pass
