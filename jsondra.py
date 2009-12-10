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
            (r"/(.*?)/(.*?)/(.*?)/", RecordHandler),
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

class RecordHandler(tornado.web.RequestHandler):
    """ Validates correct arguments to build key is passed. """
    def _initialize_key(self, keyspace, columnfamily, key):
        if "" in (keyspace, columnfamily, key):
            raise tornado.web.HTTPError(404, "missing key item")
        connection.add_pool(keyspace, self.settings.get('cassandra_pool'))
        return Key(keyspace, columnfamily, key)

    def get(self, keyspace, columnfamily, key):
        """ HTTP GET request retrieves the key if it exists, otherwise 404 """
        k = self._initialize_key(keyspace, columnfamily, key)
        r = record.Record()

        try:
            r.load(k)
            self.set_header("Content-Type", "application/json")
            self.set_header("Connection", "close")
            self.write(tornado.escape.json_encode(r))
        except:
            # key not found, throw 404
            raise tornado.web.HTTPError(404)

    def post(self, keyspace, columnfamily, key):
        """ HTTP POST will create or update the key. """
        k = self._initialize_key(keyspace, columnfamily, key)
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
            self.set_header("Content-Type", "application/json")
            self.set_header("Connection", "close")
            self.write(tornado.escape.json_encode(r))
        except:
            r.key = k
            r["_jsondra_id"] = {"keyspace": keyspace, "columnfamily": columnfamily, "key": key}
            for i in v:
                r[i] = v[i]
            r.save()
            self.set_header("Content-Type", "application/json")
            self.set_header("Connection", "close")
            self.write(tornado.escape.json_encode(r))

    def delete(self, keyspace, columnfamily, key):
        """ HTTP DELETE will delete the key """
        k = self._initialize_key(keyspace, columnfamily, key)
        r = record.Record()

        try:
            r.load(k)
            r.remove()
            self.set_header("Content-Type", "application/json")
            self.set_header("Connection", "close")

            self.write(tornado.escape.json_encode({
                "deletedItem": {
                    "keyspace": keyspace,
                    "columnfamily": columnfamily,
                    "key": key
                }}))
        except:
            raise tornado.web.HTTPError(404)

def main():
    tornado.options.parse_command_line()
    # cassandra
    # http server
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
