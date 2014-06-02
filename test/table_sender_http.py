#!/usr/bin/python

# Copyright (C) 2014 MediaMath, Inc. <http://www.mediamath.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
from time import strftime, gmtime

import logging

import httplib2
from datetime import datetime
import simplejson

from optparse import OptionParser

for path in [
    os.path.join('opt', 'qasino', 'lib'),
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib'))
]:
    if os.path.exists(os.path.join(path, '__init__.py')):
        sys.path.append(path)
        break

from util import Identity
import constants

if __name__ == "__main__":

    parser = OptionParser()

    parser.add_option("-i", "--identity", dest="identity",
                      help="Use IDENTITY as identity", metavar="IDENTITY")
    parser.add_option("-s", "--schema-version", dest="schema_version", default=0,
                      help="Use schema version VERSION", metavar="VERSION")
    parser.add_option("-H", "--hostname", dest="hostname", default=0,
                      help="Use HOSTNAME to connect to", metavar="HOSTNAME")

    #parser.add_option("-q", "--quiet",
    #                  action="store_false", dest="verbose", default=True,
    #                  help="don't print status messages to stdout")

    (options, args) = parser.parse_args()

    if options.identity != None:
        Identity.set_identity(options.identity)

    logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.INFO)


    logging.info("Sending dummy table on port %d", constants.HTTP_PORT)


    table = {}

    if int(options.schema_version) == 0:

        table = { "tablename" : "dummy",
                  "column_names" : [ "identity", "the", "quick", "brown", "fox" ],
                  "column_types" : [ "varchar", "int", "int", "varchar", "varchar" ],
                  "rows" : [ [ Identity.get_identity(), 34, 5, "yes", "no" ],
                             [ Identity.get_identity(), 1000, 321, "zanzabar", strftime("%Y-%m-%d %H:%M:%S GMT", gmtime()) ] ]
                  }

    else:
        table = { "tablename" : "dummy",
                  "column_names" : [ "identity", "the", "quick", "brown", "fox", "foo" ],
                  "column_types" : [ "varchar", "int", "int", "varchar", "varchar", "varchar" ],
                  "rows" : [ [ Identity.get_identity(), 34, 5, "yes", "no", "here I am!" ],
                             [ Identity.get_identity(), 1000, 321, "zanzabar", strftime("%Y-%m-%d %H:%M:%S GMT", gmtime()), "" ] ]
                  }


    URL = 'http://%s:%d/request?op=add_table_data' % (options.hostname, constants.HTTP_PORT)

    msg = { "op" : "add_table_data", 
            "identity" : Identity.get_identity(),
            "table" : table 
          }
    jsondata = simplejson.dumps(msg)
    h = httplib2.Http()
    resp, content = h.request(URL,
                              'POST',
                              jsondata,
                              headers={'Content-Type': 'application/json'})
    print resp
    print content
