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
from optparse import OptionParser

from twisted.internet import reactor
from twisted.internet import task

for path in [
    os.path.join('opt', 'qasino', 'lib'),
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib'))
]:
    if os.path.exists(os.path.join(path, '__init__.py')):
        sys.path.insert(0, path)
        break

from txzmq import ZmqFactory

import data_manager
import zmq_requestor
from util import Identity
import constants
import qasino_table

def send_dummy_table(zmq_requestor, schema_version):

    table = qasino_table.QasinoTable(options.tablename)

    if int(schema_version) == 0:

        table.add_column("identity", "varchar")
        table.add_column("the", "int")
        table.add_column("quick", "int")
        table.add_column("brown", "varchar")
        table.add_column("fox", "varchar")
        table.add_row( [ Identity.get_identity(), 34, 5, "yes", "no" ] )
        table.add_row( [ Identity.get_identity(), 1000, 321, "zanzabar", strftime("%Y-%m-%d %H:%M:%S GMT", gmtime()) ] )

    else:
        table.add_column("identity", "varchar")
        table.add_column("the", "int")
        table.add_column("quick", "int")
        table.add_column("brown", "varchar")
        table.add_column("fox", "varchar")
        table.add_column("foo", "varchar")
        table.add_row( [ Identity.get_identity(), 34, 5, "yes", "no", "here I am!" ] )
        table.add_row( [ Identity.get_identity(), 1000, 321, "zanzabar", strftime("%Y-%m-%d %H:%M:%S GMT", gmtime()), "" ] )

    if options.persist:
        table.set_property("persist", 1)
    if options.static:
        table.set_property("static", 1)

    zmq_requestor.send_table(table)


if __name__ == "__main__":

    parser = OptionParser()

    parser.add_option("-i", "--identity", dest="identity",
                      help="Use IDENTITY as identity", metavar="IDENTITY")
    parser.add_option("-s", "--schema-version", dest="schema_version", default=0,
                      help="Use schema version VERSION", metavar="VERSION")
    parser.add_option("-H", "--hostname", dest="hostname", default=0,
                      help="Use HOSTNAME to connect to", metavar="HOSTNAME")
    parser.add_option("-p", "--persist", dest="persist", default=False,
                      action="store_true", help="Re-apply table every generation automatically on the server")
    parser.add_option("-S", "--static", dest="static", default=False,
                      action="store_true", help="Send table as a 'static' table on the server")
    parser.add_option("-t", "--tablename", dest="tablename", default="dummy",
                      help="Use TABLENAME as the table name", metavar="TABLENAME")

    #parser.add_option("-q", "--quiet",
    #                  action="store_false", dest="verbose", default=True,
    #                  help="don't print status messages to stdout")

    (options, args) = parser.parse_args()

    if options.identity != None:
        Identity.set_identity(options.identity)


    logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.INFO)


    logging.info("Sending dummy table on port %d", constants.ZMQ_RPC_PORT)

    zmq_factory = ZmqFactory()

    # Create a zeromq requestor object.

    zmq_requestor = zmq_requestor.ZmqRequestor(options.hostname, constants.ZMQ_RPC_PORT, zmq_factory)

    # Send the table at fixed intervals

    task = task.LoopingCall(send_dummy_table, zmq_requestor, options.schema_version)
    task.start(10.0)

    # Run the event loop

    reactor.run()
