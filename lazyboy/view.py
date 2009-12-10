# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Lazyboy: Views."""

import datetime
import uuid

from cassandra.ttypes import SlicePredicate, SliceRange

from lazyboy.key import Key
from lazyboy.base import CassandraBase
from lazyboy.iterators import multigetterator, unpack
from lazyboy.record import Record
from lazyboy.connection import Client


def _iter_time(start=None, **kwargs):
    """Return a sequence which iterates time."""
    day = start or datetime.datetime.today()
    intv = datetime.timedelta(**kwargs)
    while day.year >= 1900:
        yield day.strftime('%Y%m%d')
        day = day - intv


def _iter_days(start=None):
    """Return a sequence which iterates over time one day at a time."""
    return _iter_time(start, days=1)


class View(CassandraBase):

    """A regular view."""

    def __init__(self, view_key=None, record_key=None, record_class=None):
        assert not view_key or isinstance(view_key, Key)
        assert not record_key or isinstance(record_key, Key)
        assert not record_class or isinstance(record_class, type)

        CassandraBase.__init__(self)

        self.chunk_size = 100
        self.key = view_key
        self.record_key = record_key
        self.record_class = record_class or Record
        self.reversed = 0

    def __repr__(self):
        return "%s: %s" % (self.__class__.__name__, self.key)

    def __len__(self):
        """Return the number of records in this view."""
        return self._get_cas().get_count(
            self.key.keyspace, self.key.key, self.key, self.consistency)

    def _keys(self, start_col=None, end_col=None):
        """Return keys in the view."""
        client = self._get_cas()
        assert isinstance(client, Client), \
            "Incorrect client instance: %s" % client.__class__
        last_col = start_col or ""
        end_col = end_col or ""
        chunk_size = self.chunk_size
        passes = 0
        while True:
            # When you give Cassandra a start key, it's included in the
            # results. We want it in the first pass, but subsequent iterations
            # need to the count adjusted and the first record dropped.
            fudge = int(passes > 0)
            cols = client.get_slice(
                self.key.keyspace, self.key.key, self.key,
                SlicePredicate(slice_range=SliceRange(
                        last_col, end_col, self.reversed, chunk_size + fudge)),
                self.consistency)

            if len(cols) == 0:
                raise StopIteration()

            for col in unpack(cols[fudge:]):
                yield self.record_key.clone(key=col.value)

            last_col = col.name
            passes += 1

            if len(cols) < self.chunk_size:
                raise StopIteration()

    def __iter__(self):
        """Iterate over all objects in this view."""
        return (self.record_class().load(key) for key in self._keys())

    def _record_key(self, record=None):
        """Return the column name for a given record."""
        return record.key.key if record else str(uuid.uuid1())

    def append(self, record):
        """Append a record to a view"""
        assert isinstance(record, Record), \
            "Can't append non-record type %s to view %s" % \
            (record.__class__, self.__class__)
        self._get_cas().insert(
            self.key.keyspace, self.key.key,
            self.key.get_path(column=self._record_key(record)),
            record.key.key, record.timestamp(), self.consistency)

    def remove(self, record):
        """Remove a record from a view"""
        assert isinstance(record, Record), \
            "Can't remove non-record type %s to view %s" % \
            (record.__class__, self.__class__)
        self._get_cas().remove(
            self.key.keyspace, self.key.key,
            self.key.get_path(column=self._record_key(record)),
            record.timestamp(), self.consistency)


class FaultTolerantView(View):

    """A view which ignores missing keys."""

    def __iter__(self):
        """Iterate over all objects in this view, ignoring bad keys."""
        for key in self._keys():
            try:
                yield self.record_class().load(key)
            except GeneratorExit:
                raise
            except Exception:
                pass


class BatchLoadingView(View):

    """A view which loads records in bulk."""

    def __init__(self, view_key=None, record_key=None, record_class=None):
        """Initialize the view, setting the chunk_size to a large value."""
        View.__init__(self, view_key, record_key, record_class)
        self.chunk_size = 5000

    def __iter__(self):
        """Batch load and iterate over all objects in this view."""
        keys = tuple(self._keys())
        recs = multigetterator(keys, self.consistency)

        if (self.record_key.keyspace not in recs
            or self.record_key.column_family not in
            recs[self.record_key.keyspace]):
            raise StopIteration()

        data = recs[self.record_key.keyspace][self.record_key.column_family]

        for k in keys:
            yield (self.record_class()._inject(
                self.record_key.clone(key=k.key), data[k.key]))


class PartitionedView(object):

    """A Lazyboy view which is partitioned across rows."""

    def __init__(self, view_key=None, view_class=None):
        self.view_key = view_key
        self.view_class = view_class

    def partition_keys(self):
        """Return a sequence of row keys for the view partitions."""
        return ()

    def _get_view(self, key):
        """Return an instance of a view for a partition key."""
        return self.view_class(self.view_key.clone(key=key))

    def __iter__(self):
        """Iterate over records in the view."""
        for view in (self._get_view(key) for key in self.partition_keys()):
            for record in view:
                yield record

    def _append_view(self, record):
        """Return the view which this record should be appended to.

        This defaults to the first view from partition_keys, but you
        can partition by anything, e.g. first letter of some field in
        the record.
        """
        key = iter(self.partition_keys()).next()
        return self._get_view(key)

    def append(self, record):
        """Append a record to the view."""
        return self._append_view(record).append(record)
