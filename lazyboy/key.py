# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Lazyboy: Key."""

import uuid

from cassandra.ttypes import ColumnPath, ColumnParent

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
        """Return a boolean indicating if this is a key to a supercolumn."""
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
        """Return a new ColumnPath for this key."""
        new = self._attrs()
        del new['keyspace'], new['key']
        new.update(kwargs)
        return ColumnPath(**new)

    def clone(self, **kwargs):
        """Return a clone of this key with keyword args changed"""
        return DecoratedKey(self, **kwargs)


class DecoratedKey(Key):

    """A key which decorates another.

    Any properties set in this key override properties in the parent
    key. Non-overriden properties changed in the parent key are
    changed in the child key.
    """

    def __init__(self, parent_key, **kwargs):
        self.parent_key = parent_key
        for (key, val) in kwargs.items():
            setattr(self, key, val)

    def __getattr__(self, attr):
        if hasattr(self.parent_key, attr):
            return getattr(self.parent_key, attr)

        raise AttributeError("`%s' object has no attribute `%s'" % \
                                 (self.__class__.__name__, attr))
