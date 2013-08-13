#!/usr/bin/python

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
import json_requestor
from util import Identity
import constants

def send_dummy_table(json_requestor, schema_version):

    table = {}

    if int(schema_version) == 0:

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

    json_requestor.send_table(table, persist=options.persist)


if __name__ == "__main__":

    parser = OptionParser()

    parser.add_option("-i", "--identity", dest="identity",
                      help="Use IDENTITY as identity", metavar="IDENTITY")
    parser.add_option("-s", "--schema-version", dest="schema_version", default=0,
                      help="Use schema version VERSION", metavar="VERSION")
    parser.add_option("-H", "--hostname", dest="hostname", default=0,
                      help="Use HOSTNAME to connect to", metavar="HOSTNAME")
    parser.add_option("-p", "--persist", dest="persist", default=False,
                      action="store_true", help="Use HOSTNAME to connect to", metavar="HOSTNAME")

    #parser.add_option("-q", "--quiet",
    #                  action="store_false", dest="verbose", default=True,
    #                  help="don't print status messages to stdout")

    (options, args) = parser.parse_args()

    if options.identity != None:
        Identity.set_identity(options.identity)


    logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.INFO)


    logging.info("Sending dummy table on port %d", constants.JSON_RPC_PORT)

    zmq_factory = ZmqFactory()

    # Create a json requestor object.

    json_requestor = json_requestor.JsonRequestor(options.hostname, constants.JSON_RPC_PORT, zmq_factory)

    # Send the table at fixed intervals

    task = task.LoopingCall(send_dummy_table, json_requestor, options.schema_version)
    task.start(10.0)

    # Run the event loop

    reactor.run()
