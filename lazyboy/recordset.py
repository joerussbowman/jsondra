# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Efficiently handle sets of Records."""

from itertools import ifilter

from lazyboy.key import Key
import lazyboy.iterators as itr
from lazyboy.record import Record
from lazyboy.base import CassandraBase
from lazyboy.exceptions import ErrorMissingField


def valid(records):
    """Returns True if all records in the set are valid."""
    return all(record.valid() for record in records)


def missing(records):
    """Returns a tuple indicating any fields missing from the records
    in the set."""
    return dict((r.key.key, r.missing()) for r in records if not r.valid())


def modified(records):
    """Returns a tuple of modifiedrecords in the set."""
    return tuple(ifilter(lambda r: r.is_modified(), records))


class RecordSet(CassandraBase, dict):

    """A set of Lazyboy records."""

    def __init__(self, records=None):
        """Initialize the RecordSet. Returns None."""
        CassandraBase.__init__(self)
        records = self._transform(records) if records else {}
        dict.__init__(self, records)

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
        data = itr.multigetterator(keys, self.consistency)
        for (keyspace, col_fams) in data.iteritems():
            for (col_fam, rows) in col_fams.iteritems():
                for (row_key, cols) in rows.iteritems():
                    yield record_class()._inject(
                        Key(keyspace=keyspace, column_family=col_fam,
                            key=row_key), cols)
