#!/usr/bin/python

import os
import sys
from time import strftime, gmtime
import random

import logging

from txzmq import ZmqFactory

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
import json_requestor



def random_string(start, stop):
    string = ""
    sizeof_string = random.randint(start, stop + 1)
    for x in range(sizeof_string):
        pick_a_char_index = random.randint(0, len(random_string.alphabet) - 1)
        string += random_string.alphabet[pick_a_char_index]
    return string

random_string.alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

def get_a_random_table():

    type_array = [ "TEXT", "INTEGER" ]

    nr_columns = random.randint(1, 20)
    column_names = [ random_string(1, 40) for _ in range(nr_columns) ]
    column_types = [ type_array[ random.randint(0, len(type_array) - 1) ] for _ in range(nr_columns) ]

    rows = []
    for row_index in range(random.randint(1, 300)):
        row = []
        for column_index in range(nr_columns):
            if column_types[column_index] == "TEXT":
                row.append(random_string(1, 50))
            else:
                row.append(random.randint(0, 3483839392))
        rows.append(row)

    table = { "tablename" : random_string(4, 20),
              "column_names" : column_names,
              "column_types" : column_types,
              "rows" : rows
              }
    return table


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

    #parser.add_option("-q", "--quiet",
    #                  action="store_false", dest="verbose", default=True,
    #                  help="don't print status messages to stdout")

    (options, args) = parser.parse_args()

    if options.identity != None:
        Identity.set_identity(options.identity)

    logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.INFO)


    zmq_factory = ZmqFactory()
    json_requestor = json_requestor.JsonRequestor(options.hostname, constants.JSON_RPC_PORT, zmq_factory)

    for x in range(int(options.nr_tables)):
        logging.info("Sending random table on port %d", constants.JSON_RPC_PORT)

        table = get_a_random_table()

        json_requestor.send_table(table)

