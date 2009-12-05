# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Lazyboy: Key."""

import uuid
from copy import copy

from cassandra.ttypes import ColumnPath, ColumnParent, ConsistencyLevel

from lazyboy.base import CassandraBase
from lazyboy.exceptions import ErrorIncompleteKey

class Key(ColumnParent, CassandraBase):
    """A key which determines how to reach a record."""
    def __init__(self, keyspace=None, column_family=None, key=None,
                 super_column=None):
        if not keyspace or not column_family:
            raise ErrorIncompleteKey("A required attribute was not set.")
        key = key or self._gen_uuid()

        self.keyspace, self.key = keyspace, key
        ColumnParent.__init__(self, column_family, super_column)
        CassandraBase.__init__(self)

    def _gen_uuid(self):
        """Generate a UUID for this object"""
        return uuid.uuid4().hex

    def is_super(self):
        return bool(self.super_column)

    def _attrs(self):
        """Get attributes of this key."""
        return dict((attr, getattr(self, attr)) for attr in
                    ('keyspace', 'column_family', 'key', 'super_column'))

    def __repr__(self):
        """Return a printable representation of this key."""
        return str(self._attrs())

    def __unicode__(self):
        """Return a unicode string of this key."""
        return unicode(str(self))

    def get_path(self, **kwargs):
        new = self._attrs()
        del new['keyspace'], new['key']
        new.update(kwargs)
        return ColumnPath(**new)

    def clone(self, **kwargs):
        """Return a clone of this key with keyword args changed"""
        return DecoratedKey(self, **kwargs)

class KeyRange(CassandraBase):
    def __call__(self, key, start="", finish="", count=100):
        """Return a range of keys."""
        cas = self._get_cas(key.keyspace)
        keys = cas.get_key_range(
            key.keyspace, key.column_family, start, finish, count,
            ConsistencyLevel.ONE
        )

        return [Key(keyspace=key.keyspace, 
                    column_family=key.column_family,
                    key=k) for k in keys]

get_range = KeyRange()

class DecoratedKey(Key):
    def __init__(self, parent_key, **kwargs):
        self.parent_key = parent_key
        for (k, v) in kwargs.items(): setattr(self, k, v)

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]

        if hasattr(self.parent_key, attr):
            return getattr(self.parent_key, attr)

        raise AttributeError("`%s' object has no attribute `%s'" % \
                                 (self.__class__.__name__, attr))

    def __hasattr__(self, attr):
        return attr in self.__dict__ or hasattr(self.parent_key, attr)
