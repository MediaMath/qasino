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

import logging
import re
import sqlite3
import time

from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver

import util

class SqlReceiver(LineReceiver):
    """
    A simple line based sql receiver.
    """

    def __init__(self, factory, connection_id):
        self.sql_statement = ''
        self.nr_queries = 0
        self.factory = factory
        self.connection_id = connection_id


    def lineReceived(self, line):
        """
        When a line comes in from the client appened it to the working sql statement.
        If its a complete statement (semicolon terminated), execute it.
        """

        # Exit on lines with just ctrl-d and/or ctrl-c

        m = re.search(r"^\s*[" + chr(4) + chr(3) + "]+\s*$", line, flags=re.IGNORECASE)

        if m != None:
            self.sendLine("Bye!")
            return 1

        # Add this line to the multi-line sql statement.

        self.sql_statement += line + "\n"

        # Do we have a complete sql statement?

        if sqlite3.complete_statement(self.sql_statement):
            
            query_id = self.factory.data_manager.get_query_id()

            query_start = time.time()

            logging.info("SqlReceiver: (%d:%d) SQL received: %s", self.connection_id, query_id, self.sql_statement.rstrip())

            
            # Enqueue
            d = self.factory.data_manager.async_validate_and_route_query(self.sql_statement, query_id)

            d.addCallback(self.sql_complete_callback, query_id, query_start)


    def sql_complete_callback(self, result, query_id, query_start):
        """
        Called when a sql statement completes.
        """

        retval = result["retval"]
        data          = result["data"]          if "data"          in result else None
        error_message = result["error_message"] if "error_message" in result else ''
        max_widths    = result["max_widths"]    if "max_widths"    in result else None

        if retval == 0 and data != None:
            util.pretty_print_table(self, data, max_widths=max_widths)

        if retval != 0:
            logging.info("SqlReceiver: (%d:%d) SQL error: %s", self.connection_id, query_id, error_message)
            self.sendLine("SQL error: %s" % error_message)
        else:
            logging.info("SqlReceiver: (%d:%d) SQL completed (%.02f seconds)", self.connection_id, query_id, time.time() - query_start)

        self.nr_queries += 1
        self.factory.nr_queries += 1
        self.sql_statement = ''



    def connectionMade(self):
        logging.info("SqlReceiver: (%d) Connection received!", self.connection_id)

    def connectionLost(self, reason):
        if self.sql_statement != '':
            logging.info("SqlReceiver: (%d) Connection terminated! %d queries SQL: \n%s", self.connection_id, self.nr_queries, self.sql_statement)
        else:
            logging.info("SqlReceiver: (%d) Connection terminated! %d queries", self.connection_id, self.nr_queries)



class SqlReceiverFactory(Factory):

    def __init__(self, data_manager):
        self.connection_id = 0
        self.nr_statements = 0
        self.data_manager = data_manager
        self.nr_queries = 0

    def set_backend_connection(self, backend_connection):
        self.backend_connection = backend_connection

    def buildProtocol(self, addr):
        self.connection_id += 1
        return SqlReceiver(self, self.connection_id)
        #return self.data_manager.get_sql_receiver(self, self.connection_id)

