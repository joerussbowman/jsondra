# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Lazyboy: Record."""

import time
from itertools import ifilterfalse as filternot

from cassandra.ttypes import Column, SuperColumn, ColumnOrSuperColumn, \
    SlicePredicate, SliceRange, ConsistencyLevel

from lazyboy.base import CassandraBase
from lazyboy.key import Key
from lazyboy.exceptions import ErrorMissingField, ErrorNoSuchRecord, \
    ErrorMissingKey, ErrorInvalidValue


class Record(CassandraBase, dict):
    """An object backed by a record in Cassandra."""
    # A tuple of items which must be present for the object to be valid
    _required = ()

    def __init__(self, *args, **kwargs):
        dict.__init__(self)
        CassandraBase.__init__(self)

        self._clean()

        if args or kwargs:
            self.update(*args, **kwargs)

    def valid(self):
        """Return a boolean indicating whether the record is valid."""
        return len(self.missing()) == 0

    def missing(self):
        """Return a tuple of required items which are missing."""
        return tuple(filternot(self.get, self._required))

    def is_modified(self):
        """Return True if the record has been modified since it was loaded."""
        return bool(len(self._modified) + len(self._deleted))

    def _clean(self):
        """Remove every item from the object"""
        map(self.__delitem__, self.keys())
        self._original, self._columns = {}, {}
        self._modified, self._deleted = {}, {}
        self.key = None

    def update(self, arg=None, **kwargs):
        """Update the object as with dict.update. Returns None."""
        if arg:
            if hasattr(arg, 'keys'):
                for key in arg: self[key] = arg[key]
            else:
                for key, val in arg: self[key] = val

        if kwargs:
            for key in kwargs: self[key] = kwargs[key]

    def sanitize(self, value):
        """Return a value appropriate for sending to Cassandra."""
        if value.__class__ is unicode:
            value = value.encode('utf-8')
        return str(value)

    def __repr__(self):
        """Return a printable representation of this record."""
        return "%s: %s" % (self.__class__.__name__, dict.__repr__(self))

    def timestamp(self):
        """Return a GMT UNIX timestamp."""
        return int(time.mktime(time.gmtime()))

    def __setitem__(self, item, value):
        """Set an item, storing it into the _columns backing store."""
        if value is None:
            raise ErrorInvalidValue("You may not set an item to None.")

        value = self.sanitize(value)
        # If this doesn't change anything, don't record it
        _orig = self._original.get(item)
        if _orig and _orig.value == value:
            return

        dict.__setitem__(self, item, value)

        if item not in self._columns:
            self._columns[item] = Column(name=item)

        col = self._columns[item]

        if item in self._deleted:
            del self._deleted[item]

        self._modified[item] = True
        col.value, col.timestamp = value, self.timestamp()

    def __delitem__(self, item):
        dict.__delitem__(self, item)
        # Don't record this as a deletion if it wouldn't require a remove()
        self._deleted[item] = item in self._original
        if item in self._modified: del self._modified[item]
        del self._columns[item]

    def _inject(self, key, columns):
        """Inject columns into the record after they have been fetched.."""
        self.key = key
        if isinstance(columns, list):
            columns = dict((col.name, col) for col in columns)

        self._original = columns
        self.revert()
        return self

    def _marshal(self):
        """Marshal deleted and changed columns."""
        return {'deleted': tuple(self.key.get_path(column=col)
                                 for col in self._deleted.keys()),
                'changed': tuple(self._columns[key]
                                 for key in self._modified.keys())}

    def load(self, key):
        """Load this record from primary key"""
        assert isinstance(key, Key), "Bad key passed to load()"
        self._clean()

        _slice = self._get_cas(key.keyspace).get_slice(
            key.keyspace, key.key, key,
            SlicePredicate(slice_range=SliceRange(start="", finish="")),
            ConsistencyLevel.ONE)

        if not _slice:
            raise ErrorNoSuchRecord("No record matching key %s" % key)

        self._inject(key,
                     dict([(obj.column.name, obj.column) for obj in _slice]))
        return self

    def save(self):
        """Save the record, returns self."""
        if not self.valid():
            raise ErrorMissingField("Missing required field(s):",
                                    self.missing())

        if not hasattr(self, 'key') or not self.key:
            raise ErrorMissingKey("No key has been set.")

        assert isinstance(self.key, Key), "Bad record key in save()"

        client = self._get_cas()

        # Marshal changes
        (deleted, changed) = self._marshal().values()

        # Delete items

        for path in deleted:
            client.remove(self.key.keyspace, self.key.key, path,
                          self.timestamp(), ConsistencyLevel.ONE)
        self._deleted.clear()

        # Update items
        if changed:
            client.batch_insert(*self._get_batch_args(self.key, changed))
            self._modified.clear()
        self._original = self._columns.copy()

        return self

    def _get_batch_args(self, key, columns):
        """Return a BatchMutation for the given key and columns."""
        cfmap = {}
        if not key.is_super():
            cols = [ColumnOrSuperColumn(column=col) for col in columns]
        else:
            scol = SuperColumn(name=key.super_column,
                               columns=columns)
            cols = [ColumnOrSuperColumn(super_column=scol)]

        return (key.keyspace, key.key, {key.column_family: cols},
                ConsistencyLevel.ONE)

    def remove(self):
        """Remove this record from Cassandra."""
        self._get_cas().remove(self.key.keyspace, self.key.key,
                               self.key.get_path(), self.timestamp(),
                               ConsistencyLevel.ONE)

    def revert(self):
        """Revert changes, restoring to the state we were in when loaded."""
        for col in self._original.values():
            dict.__setitem__(self, col.name, col.value)
            self._columns[col.name] = col

        self._modified, self._deleted = {}, {}
