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

import logging

from txzmq import ZmqFactory

from datetime import datetime

from optparse import OptionParser

for path in [
    os.path.join('opt', 'qasino', 'lib'),
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib'))
]:
    if os.path.exists(os.path.join(path, '__init__.py')):
        sys.path.append(path)
        break

import util
import constants
import zmq_requestor
import qasino_table


if __name__ == "__main__":

    parser = OptionParser()

    parser.add_option("-i", "--identity", dest="identity",
                      help="Use IDENTITY as identity", metavar="IDENTITY")
    parser.add_option("-s", "--schema-version", dest="schema_version", default=0,
                      help="Use schema version VERSION", metavar="VERSION")
    parser.add_option("-H", "--hostname", dest="hostname", default=0,
                      help="Use HOSTNAME to connect to", metavar="HOSTNAME")
    parser.add_option("-n", "--nr-tables", dest="nr_tables", default=3,
                      help="Number of random tables to send", metavar="NUM")
    parser.add_option("-p", "--persist", dest="persist", default=False, action="store_true",
                      help="Set the persistent option on the table(s)")

    #parser.add_option("-q", "--quiet",
    #                  action="store_false", dest="verbose", default=True,
    #                  help="don't print status messages to stdout")

    (options, args) = parser.parse_args()

    if options.identity != None:
        util.Identity.set_identity(options.identity)

    logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.INFO)


    zmq_factory = ZmqFactory()
    zmq_requestor = zmq_requestor.ZmqRequestor(options.hostname, constants.ZMQ_RPC_PORT, zmq_factory)

    for x in range(int(options.nr_tables)):

        table = qasino_table.get_a_random_table()
        if options.persist:
            table.set_property("persist", 1)

        logging.info("Sending random table of %d rows on port %d", table.get_nr_rows(), constants.ZMQ_RPC_PORT)

        zmq_requestor.send_table(table)

