"""library to handle helix exceptions"""


class HelixException(Exception):
    """Base helix exception"""
    pass


class HelixAlreadyExistsException(HelixException):
    """Exception is thrown when an entry in helix already exists"""
    pass


class HelixDoesNotExistException(HelixException):
    """Exception is thrown when an entry in helix does not exist"""
    pass
