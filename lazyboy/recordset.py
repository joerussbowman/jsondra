# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Efficiently handle sets of Records."""

import time
from UserDict import UserDict
from itertools import groupby
from operator import attrgetter

from cassandra.ttypes import ColumnParent, SlicePredicate, SliceRange, \
    ConsistencyLevel

from lazyboy.key import Key
from lazyboy.record import Record
from lazyboy.base import CassandraBase
from lazyboy.exceptions import ErrorMissingKey, ErrorMissingField

def valid(records):
    """Returns True if all records in the set are valid."""
    return all([record.valid() for record in records])

def missing(records):
    """Returns a tuple indicating any fields missing from the records
    in the set."""
    return dict([[r.key.key, r.missing()] for r in records if not r.valid()])

def modified(records):
    """Returns a tuple of modifiedrecords in the set."""
    return tuple(filter(lambda r: r.is_modified(), records))


class RecordSet(CassandraBase, UserDict):
    """A set of Lazyboy records."""

    def __init__(self, records=None):
        """Initialize the RecordSet. Returns None."""
        CassandraBase.__init__(self)
        records = self._transform(records) if records else {}
        UserDict.__init__(self, records)

    def _transform(self, records):
        """Return a dict keyed by record key for sequence of Records."""
        if not records:
            return {}

        assert hasattr(records, '__iter__')

        return dict((record.key.key, record) for record in records)

    def append(self, record):
        """Append a new record to the set."""
        return self.__setitem__(record.key.key, record)

    def save(self):
        """Save all records.

        FIXME: This is really pretty terrible, but until we have batch
        delete and row-spanning mutations, this is as good as it can
        be. Except for SuperColumns."""

        records = modified(self.itervalues())
        if not valid(records):
            raise ErrorMissingField("Missing required field(s):",
                                    missing(records))

        for record in records:
            record.save()
        return self


class KeyRecordSet(RecordSet):
    """A set of Records defined by record key. Records are batch loaded."""

    def __init__(self, keys=None, record_class=None):
        """Initialize the set."""
        record_class = record_class or Record
        records = self._batch_load(record_class, keys) if keys else None
        RecordSet.__init__(self, records)

    def _batch_load(self, record_class, keys):
        """Return an iterator of records for the given keys."""
        for keyspace, keys in groupby(keys,attrgetter('keyspace')):
            client = self._get_cas(keyspace)
            for cf, by_cf in groupby(keys, attrgetter('column_family')):
                records = client.multiget_slice(
                    keyspace, map(attrgetter('key'), by_cf), ColumnParent(cf),
                    SlicePredicate(slice_range=SliceRange("", "", 0, 9999)),
                    ConsistencyLevel.ONE)

                for (r_key, r_cols) in records.iteritems():
                    yield record_class()._inject(
                        Key(keyspace=keyspace, column_family=cf, key=r_key),
                        map(attrgetter('column'), r_cols))
