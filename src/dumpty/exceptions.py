"""Dumpty exception classes.
"""


class DumptyException(Exception):
    """The base Dumpty Exception that all other exception classes extend."""


class ValidationException(DumptyException):
    """Exceptions indicating bad input"""


class SQLException(DumptyException):
    """Exceptions indicating error from SQL server"""
