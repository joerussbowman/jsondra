# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

"""Lazyboy: Exceptions."""


class LazyboyException(Exception):
    """Base exception class which all Lazyboy exceptions extend."""
    pass


class ErrorNotSupported(LazyboyException):
    """Raised when the operation is unsupported."""
    pass


class ErrorMissingField(LazyboyException):
    """Raised in Record.save when a record is incomplete."""
    pass


class ErrorInvalidValue(LazyboyException):
    """Raised when a Record's item is set to None."""
    pass


class ErrorMissingKey(LazyboyException):
    """Raised when a key is needed, but not available."""
    pass


class ErrorIncompleteKey(LazyboyException):
    """Raised when there isn't enough information to create a key."""
    pass


class ErrorNoSuchRecord(LazyboyException):
    """Raised when a nonexistent record is requested."""
    pass


class ErrorCassandraClientNotFound(LazyboyException):
    """Raised when there is no client for a requested keyspace."""
    pass


class ErrorThriftMessage(LazyboyException):
    """Raised when there is a Thrift transport error."""
    pass


class ErrorCassandraNoServersConfigured(LazyboyException):
    """Raised when Client has no servers, but was asked for one."""
    pass


class ErrorImmutable(LazyboyException):
    """Raised on an attempt to modify an immutable object."""
    pass
