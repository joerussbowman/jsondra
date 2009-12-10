# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Lazyboy: Record."""

import time
import copy
from itertools import ifilterfalse as filternot

from cassandra.ttypes import Column, SuperColumn

from lazyboy.base import CassandraBase
from lazyboy.key import Key
import lazyboy.iterators as iterators
import lazyboy.exceptions as exc


class Record(CassandraBase, dict):

    """An object backed by a record in Cassandra."""

    # A tuple of items which must be present for the object to be valid
    _required = ()

    # The keyspace this record should be saved in
    _keyspace = None

    # The column family this record should be saved in
    _column_family = None

    # Indexes which this record should be added to on save
    _indexes = []

    # Denormalized copies of this record
    _mirrors = []

    def __init__(self, *args, **kwargs):
        dict.__init__(self)
        CassandraBase.__init__(self)

        self._clean()

        if args or kwargs:
            self.update(*args, **kwargs)

    def make_key(self, key=None, super_column=None, **kwargs):
        """Return a new key."""
        args = {'keyspace': self._keyspace,
                'column_family': self._column_family,
                'key': key,
                'super_column': super_column}
        args.update(**kwargs)
        return Key(**args)

    def default_key(self):
        """Return a default key for this record."""
        raise exc.ErrorMissingKey("There is no key set for this record.")

    def set_key(self, key, super_column=None):
        """Set the key for this record."""
        self.key = self.make_key(key=key, super_column=super_column)
        return self

    def get_indexes(self):
        """Return indexes this record should be stored in."""
        return [index() if isinstance(index, type) else index
                for index in self._indexes]

    def get_mirrors(self):
        """Return mirrors this record should be stored in."""
        return [mirror if isinstance(mirror, type) else mirror
                for mirror in self._mirrors]

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
                for key in arg:
                    self[key] = arg[key]
            else:
                for (key, val) in arg:
                    self[key] = val

        if kwargs:
            for key in kwargs:
                self[key] = kwargs[key]
        return self

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
            raise exc.ErrorInvalidValue("You may not set an item to None.")

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
        if item in self._modified:
            del self._modified[item]
        del self._columns[item]

    def _inject(self, key, columns):
        """Inject columns into the record after they have been fetched.."""
        self.key = key
        if not isinstance(columns, dict):
            columns = dict((col.name, col) for col in iter(columns))

        self._original = columns
        self.revert()
        return self

    def _marshal(self):
        """Marshal deleted and changed columns."""
        return {'deleted': tuple(self.key.get_path(column=col)
                                 for col in self._deleted.keys()),
                'changed': tuple(self._columns[key]
                                 for key in self._modified.keys())}

    def load(self, key, consistency=None):
        """Load this record from primary key"""
        if not isinstance(key, Key):
            key = self.make_key(key)

        self._clean()
        consistency = consistency or self.consistency

        columns = iterators.slice_iterator(key, consistency)

        self._inject(key, dict([(column.name, column) for column in columns]))
        return self

    def save(self, consistency=None):
        """Save the record, returns self."""
        if not self.valid():
            raise exc.ErrorMissingField("Missing required field(s):",
                                        self.missing())

        if not hasattr(self, 'key') or not self.key:
            self.key = self.default_key()

        assert isinstance(self.key, Key), "Bad record key in save()"

        # Marshal and save changes
        changes = self._marshal()
        self._save_internal(self.key, changes, consistency)

        try:
            try:
                # Save mirrors
                for mirror in self.get_mirrors():
                    self._save_internal(mirror.mirror_key(self), changes,
                                        consistency)
            finally:
                # Update indexes
                for index in self.get_indexes():
                    index.append(self)
        finally:
            # Clean up internal state
            if changes['changed']:
                self._modified.clear()
            self._original = copy.deepcopy(self._columns)

        return self

    def _save_internal(self, key, changes, consistency=None):
        """Internal save method."""

        consistency = consistency or self.consistency
        client = self._get_cas(key.keyspace)
        # Delete items
        for path in changes['deleted']:
            client.remove(key.keyspace, key.key, path,
                          self.timestamp(), consistency)
        self._deleted.clear()

        # Update items
        if changes['changed']:
            client.batch_insert(*self._get_batch_args(
                    key, changes['changed'], consistency))

    def _get_batch_args(self, key, columns, consistency=None):
        """Return a BatchMutation for the given key and columns."""
        consistency = consistency or self.consistency

        if key.is_super():
            columns = [SuperColumn(name=key.super_column, columns=columns)]

        return (key.keyspace, key.key,
                {key.column_family: tuple(iterators.pack(columns))},
                consistency)

    def remove(self, consistency=None):
        """Remove this record from Cassandra."""
        consistency = consistency or self.consistency
        self._get_cas().remove(self.key.keyspace, self.key.key,
                               self.key.get_path(), self.timestamp(),
                               consistency)
        self._clean()
        return self

    def revert(self):
        """Revert changes, restoring to the state we were in when loaded."""
        for col in self._original.values():
            dict.__setitem__(self, col.name, col.value)
            self._columns[col.name] = col

        self._modified, self._deleted = {}, {}


class MirroredRecord(Record):

    """A mirrored (denormalized) record."""

    def mirror_key(self, parent_record):
        """Return the key this mirror should be saved into."""
        assert isinstance(parent_record, Record)
        raise exc.ErrorMissingKey("Please implement a mirror_key method.")

    def save(self):
        """Refuse to save this record."""
        raise exc.ErrorImmutable("Mirrored records are immutable.")
