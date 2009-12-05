# -*- coding: utf-8 -*-
#
# Lazyboy: Exceptions
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

class LazyboyException(Exception):
    pass


class ErrorNotSupported(LazyboyException):
    pass


class ErrorMissingField(LazyboyException):
    pass


class ErrorInvalidField(LazyboyException):
    pass


class ErrorInvalidValue(LazyboyException):
    pass


class ErrorMissingKey(LazyboyException):
    pass


class ErrorIncompleteKey(LazyboyException):
    pass


class ErrorNoSuchRecord(LazyboyException):
    pass


class ErrorUnknownKeyspace(LazyboyException):
    pass


class ErrorCassandraClientNotFound(LazyboyException):
    pass


class ErrorThriftMessage(LazyboyException):
    pass


class ErrorCassandraNoServersConfigured(LazyboyException):
    pass
