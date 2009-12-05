#!/usr/bin/env python
#
# Copyright 2009 Joseph Bowman
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

__author__="jbowman"
__date__ ="$Dec 5, 2009 11:10:21 AM$"

import logging
import datetime
import os.path
import tornado.escape
import tornado.httpserver
import tornado.options
import tornado.web
import tornado.template

from lazyboy import *
from lazyboy import record
from lazyboy.key import Key

from tornado.options import define, options

define("port", default=8001, help="run on the given port", type=int)
define("cassandra_pool", default="127.0.0.1:9160", multiple=True,
    help="Cassandra hosts for pool")
define("debug", default=False, help="turn debugging on or off")

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", MainHandler),
            (r"/record/get/?", GetRecordHandler),
            (r"/record/put/?", PutRecordHandler),
            (r"/record/delete/?", DeleteRecordHandler),
        ]
        if type(options.cassandra_pool) is not "list":
            cassandra_pool = [options.cassandra_pool]
        else:
            cassandra_pool = options.cassandra_pool
        settings = dict(
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            cassandra_pool=cassandra_pool,
            debug=True,
        )
        tornado.web.Application.__init__(self, handlers, **settings)

class JsondraRequestHandler(tornado.web.RequestHandler):
    """ Base class for Jsondra requests """
    def _check_args(self):
        # check for attributes. This makes it easier for anyone who wants to
        # superclass this handler to create handlers with predefined keyspaces
        # and/or column families
        if not hasattr(self, 'keyspace'):
            self.keyspace = self.get_argument("ks", None)
        if not hasattr(self, 'columnfamily'):
            self.columnfamily = self.get_argument("cf", None)
        if not hasattr(self, 'key'):
            self.key = self.get_argument("k", None)

        # 500 error if anything missing.
        # TODO: add debug switch handling, if debug is on then spit out an
        # error for each missing item
        if None in (self.keyspace, self.columnfamily, self.key):
            raise tornado.web.HTTPError(500, "Request must include a keyspace, columnfamily, and key.")

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('Jsondra - The http/json interface to Cassandra')


class GetRecordHandler(JsondraRequestHandler):
    """ Handler used for getting records. Returns 404 on keys not found. """
    def get(self):
        self._check_args()
        
        connection.add_pool(self.keyspace, self.settings.get('cassandra_pool'))
        
        k = Key(self.keyspace, self.columnfamily, self.key)
        r = record.Record()
        try:
            r.load(k)
            self.write(tornado.escape.json_encode(r))
        except:
            # key not found, throw 404
            raise tornado.web.HTTPError(404)


class PutRecordHandler(JsondraRequestHandler):
    """ Handler used for adding records. """
    def get(self):
        self._check_args()
        
        connection.add_pool(self.keyspace, self.settings.get('cassandra_pool'))

        k = Key(self.keyspace, self.columnfamily, self.key)
        r = record.Record()

        try:
            v = tornado.escape.json_decode(self.get_argument("v"))
        except:
            raise tornado.web.HTTPError(500, "missing or invalid value")

        # wrapped in try in order to catch and modify existing keys
        try:
            r.load(k)
            # delete any items removed
            for i in r:
                if not i in v:
                    del r[v]
            for i in v:
                r[i] = v[i]

            r.save()
            # return what r is now, so application can confirm
            self.write(tornado.escape.json_encode(r))
        except:
            r.key = k
            for i in v:
                r[i] = v[i]
            r.save()
            self.write(tornado.escape.json_encode(r))

class DeleteRecordHandler(JsondraRequestHandler):
    def get(self):
        self._check_args()
        
        k = Key(self.keyspace, self.columnfamily, self.key)
        r = record.Record()
        try:
            r.load(k)
            r.remove()
            self.write('{"deleteItem": "success"}')
        except:
            raise tornado.web.HTTPError(404, "key not found")

def main():
    tornado.options.parse_command_line()
    # cassandra
    # http server
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
