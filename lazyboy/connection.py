# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Chris Goffinet <goffinet@digg.com>
# Author: Ian Eure <ian@digg.com>
#

"""Lazyboy: Connections."""

import random
import os
import threading

from cassandra import Cassandra
from thrift import Thrift
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol

import lazyboy.exceptions as exc

_SERVERS = {}
_CLIENTS = {}


def add_pool(name, servers):
    """Add a connection."""
    _SERVERS[name] = servers


def get_pool(name):
    """Return a client for the given pool name."""
    key = str(os.getpid()) + threading.currentThread().getName() + name
    if key in _CLIENTS:
        return _CLIENTS[key]

    try:
        _CLIENTS[key] = Client(_SERVERS[name])
        return _CLIENTS[key]
    except Exception:
        raise exc.ErrorCassandraClientNotFound(
            "Pool `%s' is not defined." % name)


class Client(object):

    """A wrapper around the Cassandra client which load-balances."""

    def __init__(self, servers):
        """Initialize the client."""
        self._servers = servers
        self._clients = [s for s in [self._build_server(*server.split(":")) \
                                         for server in servers] if s]
        self._current_server = random.randint(0, len(self._clients))

    def _build_server(self, host, port):
        """Return a client for the given host and port."""
        try:
            socket = TSocket.TSocket(host, int(port))
            # socket.setTimeout(200)
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
            client = Cassandra.Client(protocol)
            client.transport = transport
            return client
        except Exception:
            return None

    def _get_server(self):
        """Return the next server (round-robin) from the list."""
        if self._clients is None or len(self._clients) == 0:
            raise exc.ErrorCassandraNoServersConfigured

        next_server = self._current_server % len(self._clients)
        self._current_server += 1
        return self._clients[next_server]

    def list_servers(self):
        """Return all servers we know about."""
        return self._clients

    def _connect(self, client):
        """Connect to Cassandra if not connected."""
        if client.transport.isOpen():
            return True

        try:
            client.transport.open()
            return True
        except Thrift.TException, texc:
            if texc.message:
                message = texc.message
            else:
                message = "Transport error, reconnect"
            client.transport.close()
            raise exc.ErrorThriftMessage(message)
        except Exception:
            client.transport.close()

        return False

    def __getattr__(self, attr):
        """Wrap every __func__ call to Cassandra client and connect()."""

        def func(*args, **kwargs):
            """Wrapper function."""
            client = self._get_server()
            if self._connect(client):
                try:
                    return getattr(client, attr).__call__(*args, **kwargs)
                except Thrift.TException, texc:
                    if texc.message:
                        message = texc.message
                    else:
                        message = "Transport error, reconnect"
                    client.transport.close()
                    raise exc.ErrorThriftMessage(message)
                except Exception:
                    client.transport.close()
                    raise

        return func
