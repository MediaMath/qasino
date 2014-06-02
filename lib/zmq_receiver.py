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

from txzmq import ZmqFactory, ZmqEndpoint, ZmqEndpointType, ZmqREPConnection
import logging
import json
import time
import sqlite3

from util import Identity
import qasino_table

class ZmqReceiver(ZmqREPConnection):

    def __init__(self, port, zmq_factory, data_manager):
        
        self.data_manager = data_manager

        endpoint = ZmqEndpoint(ZmqEndpointType.bind, "tcp://*:%d" % port)

        ZmqREPConnection.__init__(self, zmq_factory, endpoint)
                                  
    def gotMessage(self, messageId, *messageParts):

        try:
            obj = json.loads(messageParts[0])
        except Exception as e:
            logging.info("ZmqReceiver: ERROR failed to get/parse content of POST: %s", str(e))
            response_meta = { "response_op" : "error", "error_message" : "Failed to parse JSON message: %s" % str(e), "identity" : Identity.get_identity() }
            self.reply(messageId, json.dumps(response_meta))
            return

        response_meta = { "response_op" : "error", "identity" : Identity.get_identity(), "error_message" : "Unspecified error" }

        if obj == None or obj["op"] == None:
            logging.error("ZmqReceiver: Error, unrecognized message.")
            response_meta = { "response_op" : "error", "error_message" : "Unrecognized request", "identity" : Identity.get_identity() }
            self.reply(messageId, json.dumps(response_meta))

        elif obj["op"] == "get_table_list":
            #logging.info("ZmqReceiver: Got request for table list.")
            response_meta = { "response_op" : "tables_list", "identity" : Identity.get_identity() }
            response_data = self.data_manager.get_table_list()
            self.reply(messageId, json.dumps(response_meta), json.dumps(response_data))

        elif obj["op"] == "add_table_data":
            #logging.info("ZmqReceiver: Got request to add data.")
            #print "Got request: ", obj
            table = qasino_table.QasinoTable()
            err = table.from_obj(obj)
            if err is not None:
                errmsg = "Invalid input format: " + str(err)
                logging.info("ZmqReceiver: " + errmsg)
                response_meta = { "response_op" : "error", "identity" : Identity.get_identity(), "error_message" : errmsg }
            else:
                response_meta = { "response_op" : "ok", "identity" : Identity.get_identity() }
                try:
                    if table.get_property("static"):
                        self.data_manager.sql_backend_writer_static.async_add_table_data(table, table.get_property("identity"))
                    else:
                        self.data_manager.sql_backend_writer.async_add_table_data(table, table.get_property("identity"))
                except Exception as e:
                    response_meta = { "response_op" : "error", "identity" : util.Identity.get_identity(), "error_message" : str(e) }

            self.reply(messageId, json.dumps(response_meta))

        elif obj["op"] == "generation_signal":
            logging.info("ZmqReceiver: Got generation signal.")
            # Currently unused..
            
        elif obj["op"] == "query":
            #logging.info("ZmqReceiver: Got request for table list.")
            use_write_db = True if "use_write_db" in obj and obj["use_write_db"] else False
            if "sql" not in obj:
                response_meta = { "response_op" : "error", "error_message" : "Must specify sql", "identity" : Identity.get_identity() }
                self.reply(messageId, json.dumps(response_meta))
            else:
                try:
                    self.process_sql_statement(obj["sql"], messageId, use_write_db=use_write_db)
                except Exception as e:
                    logging.error('ZmqReceiver: Invalid message received from client: error="%s", msg="%s"', str(e), str(obj))
                    response_meta = { "response_op" : "error", "error_message" : str(e), "identity" : Identity.get_identity() }
                    self.reply(messageId, json.dumps(response_meta))

                # Response is handled in the callback.
                return

        else:
            logging.error("ZmqReceiver: Error, unrecognized op '%s'", obj["op"])
            response_meta = { "response_op" : "error", "identity" : Identity.get_identity(), "error_message" : "Unrecognized op '%s'" % obj["op"] }
            self.reply(messageId, json.dumps(response_meta))


    
    def process_sql_statement(self, sql_statement, messageId, use_write_db=False):

        query_id = self.data_manager.get_query_id()

        query_start = time.time()

        logging.info("ZmqReceiver: (%d) SQL received: %s", query_id, sql_statement.rstrip())

        if not sqlite3.complete_statement(sql_statement):

            # Try adding a semicolon at the end.
            sql_statement = sql_statement + ";"

            if not sqlite3.complete_statement(sql_statement):
                return self.sql_complete_callback( { "retval" : 1, 
                                                     "error_message" : "Incomplete sql statement" },
                                                   query_id, query_start )

            # else continue

        # Enqueue
        d = self.data_manager.async_validate_and_route_query(sql_statement, query_id, use_write_db=use_write_db)

        d.addCallback(self.sql_complete_callback, query_id, query_start, messageId)

        
    def sql_complete_callback(self, result, query_id, query_start, messageId):
        """
        Called when a sql statement completes.
        """

        # To start just our identity.
        response_meta = { "identity" : Identity.get_identity() }

        retval = result["retval"]
        error_message = ''
        if "error_message" in result:
                error_message = str(result["error_message"])

        # Success?

        if retval == 0 and "data" in result:

            response_meta["response_op"] = "result_table"
            response_meta["table"] = result["data"]

            if "max_widths" in result:
                response_meta["max_widths"] = result["max_widths"]

        # Or error?

        if retval != 0:
            logging.info("ZmqReceiver: (%d) SQL error: %s", query_id, error_message)

            response_meta["response_op"] = "error"
            response_meta["error_message"] = error_message

        else:
            logging.info("ZmqReceiver: (%d) SQL completed (%.02f seconds)", query_id, time.time() - query_start)

        # Send the response!

        self.reply(messageId, json.dumps(response_meta))
