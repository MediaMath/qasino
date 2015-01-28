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
import signal
import cmd
import readline
import json
import re
import sqlite3
from optparse import OptionParser
import zmq
import requests
import getpass

for path in [
    os.path.join('opt', 'qasino', 'lib'),
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib'))
]:
    if os.path.exists(os.path.join(path, '__init__.py')):
        sys.path.append(path)
        break

import zmq_requestor
import constants
import util

class QasinoZMQConnection(object):
    def __init__(self, options):
        self.options = options
        self.socket = None

    def connect(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("tcp://%s:%s" % (self.options.hostname, self.options.port))

    def send_and_get_response(self, json):
        if self.socket == None:
            return None
        self.socket.send(json)
        return self.socket.recv()


class QasinoHttpConnection(object):
    def __init__(self, options):
        self.options = options

    def connect(self):
        self.requests_conn = requests.Session()

    def send_and_get_response(self, json):
        if self.requests_conn == None:
            return None

        URL = 'https://%s:%d/request?op=query' % (self.options.hostname, self.options.port)

        request_options = { 'headers' : {'Content-Type': 'application/json'},
                            'data' : json } 

        if self.options.skip_ssl_verify:
            request_options['verify'] = False
        if self.options.username and self.options.password:
            request_options['auth'] = (self.options.username, self.options.password)

        try:
            response = self.requests_conn.post(URL, **request_options)
        except Exception as e:
            print "ERROR: failed to send HTTPS POST to '%s': %s" % (URL, str(e))
            return None

        return response.text

class QasinoCmd(cmd.Cmd):

    multiline_prompt = '      > '
    default_prompt   = 'qasino> '
    prompt = default_prompt

    sql_statement = ''

    def __init__(self, conn):
        self.conn = conn
        self.use_write_db = False
        cmd.Cmd.__init__(self)

    def send_query(self, sql_statement, use_write_db=False):

        request_meta = { "op" : "query", "sql" : sql_statement,
                         "use_write_db" : use_write_db }

        response = self.conn.send_and_get_response(json.dumps(request_meta))
        if response == None:
            print "ERROR: error communicating with server"
            return

        try:
            obj = json.loads(response)
        except Exception as e:
            print "ERROR: unable to parse response from server: %s: %s" % (response, str(e))
            return

        if obj == None or "response_op" not in obj:
            print "ERROR: invalid response from server: ", obj

        elif obj["response_op"] == "error":
            if "error_message" in obj:
                print "ERROR: %s" % str(obj["error_message"])
            else:
                print "ERROR: Unknown error from server."

        elif obj["response_op"] == "result_table":

            max_widths = obj["max_widths"] if "max_widths" in obj else None

            util.pretty_print_table(self, obj["table"], max_widths=max_widths)
            #print "Got response: ", obj

        else:
            print "ERROR: Unknown op response from server."

    def sendLine(self, line):
        print line


    def is_set_value_true(self, value):
        m = re.search(r'^\s*(true|1|yes)\s*$', value, flags=re.IGNORECASE)
        if m:
            return 1

        m = re.search(r'^\s*(false|0|no)\s*$', value, flags=re.IGNORECASE)
        if m:
            return 0

        return -1

    def set_set(self, name, value):

        if name == 'use_write_db':
            truth = self.is_set_value_true(value)
            if truth == 1:
                print "Using write database."
                self.use_write_db = True
            elif truth == 0:
                print "Using read database."
                self.use_write_db = False
            else:
                print "Invalid value for '%s'." % (name,)
        else:
            print "Unrecognized SET statement: %s = %s" % (name, value)


    def default(self, line):
        self.sql_statement = self.sql_statement + ' ' + line.rstrip('\n')

        if sqlite3.complete_statement(self.sql_statement):

            match_set = re.search(r"^\s*set\s+(\S+)\s*=\s*'?(\S+?)'?\s*;\s*$", self.sql_statement, flags=re.IGNORECASE)
            if match_set:

                name = match_set.group(1)
                value = match_set.group(2)

                self.set_set(name, value)

            else:
                m = re.search(r"^\s*select\s+", self.sql_statement, flags=re.IGNORECASE)
                m2 = re.search(r"^\s*show\s+", self.sql_statement, flags=re.IGNORECASE)
                m3 = re.search(r"^\s*desc\s+", self.sql_statement, flags=re.IGNORECASE)

                if m or m2 or m3:
                    self.send_query(self.sql_statement, self.use_write_db)
                else:
                    print "Unrecognized statement: ", self.sql_statement

            self.reset_multiline()

        else:
            self.multiline()

    def multiline(self):
        self.prompt = self.multiline_prompt

    def reset_multiline(self):
        self.prompt = self.default_prompt
        self.sql_statement = ''
            
    def emptyline(self):
        pass

    def do_EOF(self, line):
        return True

def signal_handler(signum, frame):
    sig_names = dict((k, v) for v, k in signal.__dict__.iteritems() if v.startswith('SIG'))
    print "Caught %s.  Exiting..." % sig_names[signum]
    exit(0)

def run():

    parser = OptionParser()

    parser.add_option("-I", "--identity", dest="identity",
                      help="Use IDENTITY as identity", metavar="IDENTITY")

    parser.add_option("-H", "--hostname", dest="hostname", default='localhost',
                      help="Send table to HOSTNAME qasino server", metavar="HOSTNAME")

    parser.add_option("-p", "--port", dest="port", default=constants.HTTPS_PORT,
                      help="Use PORT for qasino server", metavar="PORT")

    parser.add_option("-t", "--use-https", dest="use_https", default=False,
                      action='store_true',
                      help="Use HTTPS transport for making queries")

    parser.add_option("-z", "--use-zmq", dest="use_zmq", default=False,
                      action='store_true',
                      help="Use ZeroMQ transport for making queries")

    parser.add_option("-u", "--username", dest="username", 
                      help="HTTPS auth username")

    parser.add_option("-w", "--password", dest="password", 
                      help="HTTPS auth password")

    parser.add_option("-s", "--skip-ssl-verify", dest="skip_ssl_verify", default=False, 
                      action="store_true",
                      help="Don't verify SSL certificates.")

    parser.add_option("-q", "--query", dest="query", 
                      action="append",
                      help="Query to execute and exit.")

    (options, args) = parser.parse_args()

    if options.identity != None:
        Identity.set_identity(options.identity)

    if options.hostname == None:
        print "Please specify a hostname to connect to."
        exit(1)

    # Can't do both.
    if options.use_https and options.use_zmq:
        print "Can not use both ZMQ and HTTPS transports."
        exit(1)

    # Pick a default.
    if not options.use_https and not options.use_zmq:
        options.use_zmq = True

    # Catch signals

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if options.use_zmq and options.port == constants.HTTPS_PORT:
        # switch to the right default port for zmq
        options.port = constants.ZMQ_RPC_PORT
    
    print "Connecting to %s %s:%s." % ('https' if options.use_https else 'zmq', 
                                       options.hostname, options.port)


    if options.use_https:

        if not options.username:
            options.username = os.environ.get('QASINO_SQLCLIENT_USERNAME')
            
        if not options.password:
            options.password = os.environ.get('QASINO_SQLCLIENT_PASSWORD')

        if options.username and not options.password:
            options.password = getpass.getpass()

        conn = QasinoHttpConnection(options)
        conn.connect()

    if options.use_zmq:
        # switch to the right default port
        if options.port == constants.HTTPS_PORT:
            options.port = constants.ZMQ_RPC_PORT

        conn = QasinoZMQConnection(options)
        conn.connect()

    if options.query:

        # Command line query string - execute and exit.
        for query in options.query:
            QasinoCmd(conn).send_query(query)

    else:

        # Enter the command loop.
        QasinoCmd(conn).cmdloop()
        
if __name__ == "__main__":
    run()

