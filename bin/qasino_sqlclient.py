#!/usr/bin/python

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

for path in [
    os.path.join('opt', 'qasino', 'lib'),
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib'))
]:
    if os.path.exists(os.path.join(path, '__init__.py')):
        sys.path.append(path)
        break

import json_requestor
import constants
import util

class QasinoCmd(cmd.Cmd):

    multiline_prompt = '      > '
    default_prompt   = 'qasino> '
    prompt = default_prompt

    sql_statement = ''

    def __init__(self, socket):
        self.socket = socket
        self.use_write_db = False
        cmd.Cmd.__init__(self)

    def send_query(self, sql_statement, use_write_db=False):
        request_meta = { "op" : "query", "sql" : sql_statement,
                         "use_write_db" : use_write_db }
        self.socket.send(json.dumps(request_meta))
        response = self.socket.recv()
        obj = json.loads(response)

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

if __name__ == "__main__":

    parser = OptionParser()

    parser.add_option("-I", "--identity", dest="identity",
                      help="Use IDENTITY as identity", metavar="IDENTITY")

    parser.add_option("-H", "--hostname", dest="hostname", default='localhost',
                      help="Send table to HOSTNAME qasino server", metavar="HOSTNAME")

    parser.add_option("-p", "--port", dest="port", default=constants.JSON_RPC_PORT,
                      help="Use PORT for qasino server", metavar="PORT")

#    parser.add_option("-t", "--use-https", dest="use_https", default=True,
#                      action='store_true',
#                      help="Use HTTPS transport for making queries")

    parser.add_option("-z", "--use-zmq", dest="use_zmq", default=True,
                      action='store_true',
                      help="Use ZeroMQ transport for making queries")

    (options, args) = parser.parse_args()

    if options.identity != None:
        Identity.set_identity(options.identity)

    if options.hostname == None:
        print "Please specify a hostname to connect to."
        exit(1)

#    if options.use_https and options.use_zmq:
#        print "Can not use both ZMQ and HTTPS transports."
#        exit(1)

    # Catch signals

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print "Connecting to %s:%d." % (options.hostname, options.port)

#    if options.use_https:
#        conn = requests.Session()
        
    if options.use_zmq:
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://%s:%s" % (options.hostname, options.port))

    QasinoCmd(socket).cmdloop()


