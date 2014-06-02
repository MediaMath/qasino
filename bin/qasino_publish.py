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

import sys
import os
from optparse import OptionParser

for path in [
    os.path.join('opt', 'qasino', 'lib'),
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib'))
]:
    if os.path.exists(os.path.join(path, '__init__.py')):
        sys.path.insert(0, path)
        break

import constants
from util import Identity
import qasino_table

def read_json_table(filehandle, options):
    """
    Read json table from the input file handle.
    """

    json_str = filehandle.read()

    try:
        import json

        json_obj = json.loads(json_str)
    except Exception as e:
        print "Failed to parse input json: {}".format(e)
        return

    table = qasino_table.QasinoTable()
    table.from_obj( { "table" : json_obj } )

    return table
    

def read_csv_table(filehandle, options):
    """
    Read csv table from the input file handle.
    """

    from csv_table_reader import CsvTableReader

    nr_errors = 0

    # Create a csv table reader object.

    csv_table_reader = CsvTableReader()

    # The csv files we'll be reading in will have this format:
    # ------------------------
    # version/options
    # tablename
    # column names (csv)
    # column types (csv)
    # column descriptions (csv)
    # data (csv)
    # ...
    # ------------------------

    # Ignore the 2nd and 5th lines.  Names in 3rd, types in 4th.  The "version" in the first line is now the options list.
    (table, error) = csv_table_reader.read_table(filehandle, None,
                                                 skip_linenos={4},
                                                 options_lineno=0,
                                                 types_lineno=3,
                                                 tablename_lineno=1,
                                                 colnames_lineno=2)

    if table == None:
        print "Failure reading csv file: {}".format(error)
        return None

    return table



if __name__ == '__main__':

    #logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
    #                    level=logging.INFO)

    parser = OptionParser()

    parser.add_option("-I", "--identity", dest="identity",
                      help="Use IDENTITY as identity", metavar="IDENTITY")

    parser.add_option("-H", "--hostname", dest="hostname", default='localhost',
                      help="Send table to HOSTNAME qasino server", metavar="HOSTNAME")

    parser.add_option("-p", "--port", dest="port", default=-1,
                      help="Use PORT to connect to", metavar="PORT")

    parser.add_option("-u", "--username", dest="username", 
                      help="HTTPS auth username")

    parser.add_option("-w", "--password", dest="password", 
                      help="HTTPS auth password")

    parser.add_option("-P", "--persist", dest="persist", default=False, action="store_true",
                      help="Set the persistent option on the table")

    parser.add_option("-S", "--static", dest="static", default=False, action="store_true",
                      help="Set the static option on the table")

    parser.add_option("-t", "--use-http", dest="use_http", default=False, action="store_true",
                      help="Use HTTP protocol to publish table.")

    parser.add_option("-s", "--use-https", dest="use_https", default=False, action="store_true",
                      help="Use HTTP over SSL/TLS protocol to publish table.")

    parser.add_option("-z", "--use-zmq", dest="use_zmq", default=False, action="store_true",
                      help="Use ZeroMQ protocol to publish table.")

    parser.add_option("-k", "--skip-ssl-verify", dest="skip_ssl_verify", default=False, action="store_true",
                      help="Don't verify SSL certificates.")

    parser.add_option("-d", "--data-format", dest="data_format", default='json',
                      help="Read data in as either 'json' or 'csv'.")

    parser.add_option("-f", "--filename", dest="filename", 
                      help="Read data from FILENAME.")


    (options, args) = parser.parse_args()

    print "Qasino publish starting"

    if options.identity != None:
        Identity.set_identity(options.identity)

    print "Identity is {}".format(Identity.get_identity())

    if options.hostname == None:
        print "Please specify a hostname to connect to."
        exit(1)

    # invalid combos
    if (options.use_zmq and options.use_https) or (options.use_http and options.use_https) or (options.use_http and options.use_zmq):
        print "Pick one of --use-https, --use-https or --use-zmq"
        exit(1);

    # default to https
    if not options.use_http and not options.use_https and not options.use_zmq:
        options.use_https = True

    # set the default port
    if options.port == -1:
        if options.use_http:
            options.port = constants.HTTP_PORT
        elif options.use_https:
            options.port = constants.HTTPS_PORT
        elif options.use_zmq:
            options.port = constants.ZMQ_RPC_PORT

    # Read from stdin by default or filename if specified.
    filename = options.filename if options.filename else "/dev/stdin"

    try:
        fh = open(filename)
    except Exception as e:
        print "Failed to open '{}': {}".format(filename, str(e))
        exit(1)

    # Are we reading a csv table or a json table?
    if options.data_format == 'json':
        table = read_json_table(fh, options)

    elif options.data_format == 'csv':
        table = read_csv_table(fh, options)

    fh.close()

    if table == None:
        print "Failed to get a valid table!"
        exit(1)

    if options.persist:
        table.set_property("persist", 1)

    if options.static:
        table.set_property("static", 1)

    properties = []
    if table.get_property('static'): properties.append(' static')
    if table.get_property('update'): properties.append(' update')
    if table.get_property('persist'): properties.append(' persist')

    print "Sending{} table '{}' to '{}:{}' ({} rows).".format(''.join(properties), table.get_tablename(), options.hostname, options.port, table.get_nr_rows())

    if options.use_zmq:

        from txzmq import ZmqFactory
        import zmq_requestor

        zmq_factory = ZmqFactory()

        # Create a json request object.

        zmq_requestor = zmq_requestor.ZmqRequestor(options.hostname, options.port, zmq_factory)

        zmq_requestor.send_table(table)


    elif options.use_http or options.use_https:

        if options.use_https:
            url_proto = 'https'
            if options.port == constants.HTTP_PORT:
                options.port = constants.HTTPS_PORT
        else:
            url_proto = 'http'

        import requests

        conn = requests.Session()

        request_options = { 'headers' : {'Content-Type': 'application/json'} }

        if options.skip_ssl_verify:
            request_options['verify'] = False
        if options.username and options.password:
            request_options['auth'] = (options.username, options.password)

        URL = '{}://{}:{}/request?op=add_table_data'.format(url_proto, options.hostname, options.port)

        jsondata = table.get_json(op="add_table_data", identity=Identity.get_identity())

        #print jsondata
    
        request_options['data'] = jsondata

        response = conn.post(URL, **request_options)
        try:
            response.raise_for_status()
        except Exception as e:
            print "ERROR: Request failed: {}".format(e)

    else:

        print "ERROR: Unknown or No transport specified."
        exit(1)

    print "Qasino publish exiting"
    exit(0)

    # END main
