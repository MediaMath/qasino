#!/usr/bin/python

import os
import sys

import logging

import requests
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

import util
import constants
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

    #parser.add_option("-q", "--quiet",
    #                  action="store_false", dest="verbose", default=True,
    #                  help="don't print status messages to stdout")

    (options, args) = parser.parse_args()

    if options.identity != None:
        util.Identity.set_identity(options.identity)

    logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.INFO)


    conn = requests.Session()

    for x in range(int(options.nr_tables)):

        table = qasino_table.get_a_random_table()

        logging.info("Sending random table of %d rows on port %d", table.get_nr_rows(), constants.HTTPS_PORT)

        URL = 'https://%s:%d/request?op=add_table_data' % (options.hostname, constants.HTTPS_PORT)

        jsondata = table.get_json(op="add_table_data", identity=util.Identity.get_identity())

        #print jsondata
    
        response = conn.post(URL, data=jsondata, headers={'Content-Type': 'application/json'})

#        resp, content = h.request(URL,
#                                  'POST',
#                                  jsondata,
#                                  headers={'Content-Type': 'application/json'})
        print response.text
