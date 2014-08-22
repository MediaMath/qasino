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
import json
import re
import time
import sqlite3
import StringIO

from pprint import pprint
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.web import http

import util
import qasino_table
import csv_table_reader

class MyLoggingHTTPChannel(http.HTTPChannel):
    def connectionMade(self):
        logging.info("HttpReceiver: Connection from '%s'.", str(self.transport.getPeer().host))
        http.HTTPChannel.connectionMade(self)

    def connectionLost(self, reason):
        logging.info("HttpReceiver: Connection close '%s'.", str(self.transport.getPeer().host))
        http.HTTPChannel.connectionLost(self, reason)

class HttpReceiver(Resource):

    def __init__(self, data_manager):
        self.data_manager = data_manager

    def render_GET(self, request):

        request.setHeader("Content-Type", "application/json")

        #pprint(request.__dict__)

        if 'op' not in request.args:
            logging.error("HttpReceiver: No op specified for GET request: %s", ','.join(request.args))
            response_meta = { "response_op" : "error", "identity" : util.Identity.get_identity(), "error_message" : "No op specified" }
            return json.dumps(response_meta)

        ## A simple name value update op.

        if request.args['op'][0] == "name_value_update":
            logging.info("HttpReceiver: Name value update.")

            if 'name' in request.args and 'value' in request.args:
                
                identity = request.args['identity'][0] if 'identity' in request.args else 'unknown'
                
                # Parse the name into '<tablename>.<column>'
                
                name = request.args['name'][0]

                m = re.search(r'^([\w_]+)\.([\w_]+)$', name)
                if m == None:
                    logging.info("HttpReciever: Invalid name in name value update: '%s'", name)
                    response_meta = { "response_op" : "error", "error_message" : "Invalid name in namve value update", "identity" : util.Identity.get_identity() }
                    return json.dumps(response_meta)

                tablename = m.group(1)
                columnname = m.group(2)
                value = request.args['value'][0]

                table = qasino_table.QasinoTable(tablename)
                table.add_column('identity', 'varchar')
                table.add_column(columnname, 'varchar')
                table.add_row( [ identity, value ] )
                table.set_property('update', 1)
                table.set_property('persist', 1)
                table.set_property('keycols', 'identity')

                self.data_manager.sql_backend_writer.async_add_table_data(table, identity)

                response_meta = { "response_op" : "ok", "identity" : util.Identity.get_identity() }
                return json.dumps(response_meta)

        ## Query op

        elif request.args['op'][0] == "query":

            # Check 'format' arg to determine output type.
            JSON = 1
            TEXT = 2
            HTML = 3  ## TODO
            format = JSON 
            try:
                if request.args['format'][0] == 'text':
                    format = TEXT
                elif request.args['format'][0] == 'html':
                    format = HTML
            except:
                pass

            # It is an error if no sql specified.
            if 'sql' not in request.args:
                logging.info("HttpReceiver: GET Query received with no sql.")
                if format == TEXT:
                    request.setHeader("Content-Type", "text/plain")
                    return "Must specify 'sql' param."
                else:
                    response_meta = { "response_op" : "error", "error_message" : "Must specify 'sql' param", "identity" : util.Identity.get_identity() }
                    return json.dumps(response_meta)


            sql = request.args['sql'][0]
            try:
                if format == TEXT:
                    self.process_sql_statement_for_text(sql, request)
                else:
                    self.process_sql_statement(sql, request)
                return NOT_DONE_YET

            except Exception as e:
                logging.error('HttpReceiver: Error processing sql: %s: %s', str(e), sql)
                if format == TEXT:
                    request.setHeader("Content-Type", "text/plain")
                    return "Error processing sql: %s" % str(e)
                else:
                    response_meta = { "response_op" : "error", "error_message" : "Error processing sql: %s" % str(e), "identity" : util.Identity.get_identity() }
                    return json.dumps(response_meta)

        logging.error("HttpReceiver: Unknown op: %s", request.args['op'][0])
        response_meta = { "response_op" : "error", "identity" : util.Identity.get_identity(), "error_message" : "Unrecognized operation" }
        return json.dumps(response_meta)

    def render_POST(self, request):

        request.setHeader("Content-Type", "application/json")

        #pprint(request.__dict__)
        obj = dict()

        if 'op' not in request.args:
            logging.error("HttpReceiver: No op specified for POST request: %s", ','.join(request.args))
            response_meta = { "response_op" : "error", "identity" : util.Identity.get_identity(), "error_message" : "No op specified" }
            return json.dumps(response_meta)

        ## Add CSV table data op

        if request.args['op'][0] == "add_csv_table_data":
            logging.info("HttpReceiver: Add table data (CSV).")

            csv = csv_table_reader.CsvTableReader()

            (table, error) = csv.read_table(request.content, None,
                                            skip_linenos={4},
                                            options_lineno=0,
                                            types_lineno=3,
                                            tablename_lineno=1,
                                            colnames_lineno=2)

            if table == None:
                logging.info("HttpReceiver: Failure parsing csv input: {}".format(error))
                response_meta = { "response_op" : "error", "identity" : util.Identity.get_identity(), "error_message" : str(error) }
                return json.dumps(response_meta)

            response_meta = { "response_op" : "ok", "identity" : util.Identity.get_identity() }

            try:
                if table.get_property("static"):
                    self.data_manager.sql_backend_writer_static.async_add_table_data(table, table.get_property("identity"))
                else:
                    self.data_manager.sql_backend_writer.async_add_table_data(table, table.get_property("identity"))
            except Exception as e:
                response_meta = { "response_op" : "error", "identity" : util.Identity.get_identity(), "error_message" : str(e) }
            
            return json.dumps(response_meta)

        ### The rest of the ops expect a JSON post body.

        try:
            # request.content might be a temp file if the content length is over some threshold.
            # fortunately json.load() will take a file ptr or a StringIO obj.
            obj = json.load(request.content)
            #print "Received POST:"
            #print obj

        except Exception as e:
            logging.info("HttpReceiver: ERROR failed to get/parse content of POST: %s", str(e))
            response_meta = { "response_op" : "error", "identity" : util.Identity.get_identity(), "error_message" : "Could not parse POST body: %s" % str(e) }
            return json.dumps(response_meta)

        ## Table list op

        if request.args['op'][0] == "get_table_list":
            #logging.info("HttpReceiver: Got request for table list.")
            response_meta = { "response_op" : "tables_list", "identity" : util.Identity.get_identity() }
            response_data = self.data_manager.get_table_list()
            return json.dumps(response_meta) + json.dumps(response_data)

        ## Add table data op

        if request.args['op'][0] == "add_table_data":

            #logging.info("HttpReceiver: Add table data (JSON).")
            table = qasino_table.QasinoTable()
            err = table.from_obj(obj)
            if err is not None:
                errmsg = "Invalid input format: " + str(err)
                logging.info("HttpReceiver: " + errmsg)
                response_meta = { "response_op" : "error", "identity" : util.Identity.get_identity(), "error_message" : errmsg }
            else:
                response_meta = { "response_op" : "ok", "identity" : util.Identity.get_identity() }
                try:
                    if table.get_property("static"):
                        self.data_manager.sql_backend_writer_static.async_add_table_data(table, table.get_property("identity"))
                    else:
                        self.data_manager.sql_backend_writer.async_add_table_data(table, table.get_property("identity"))
                except Exception as e:
                    response_meta = { "response_op" : "error", "identity" : util.Identity.get_identity(), "error_message" : str(e) }
            
            return json.dumps(response_meta)

        ## Query op

        if request.args['op'][0] == "query":

            if 'sql' not in obj:
                response_meta = { "response_op" : "error", "error_message" : "Must specify 'sql' param", "identity" : util.Identity.get_identity() }
                logging.info("HttpReceiver: Query received with no sql.")
                return json.dumps(response_meta)

            try:
                self.process_sql_statement(obj["sql"], request)
                return NOT_DONE_YET

            except Exception as e:
                logging.error('HttpReceiver: Error processing sql: %s: %s', str(e), obj["sql"])
                response_meta = { "response_op" : "error", "error_message" : str(e), "identity" : util.Identity.get_identity() }
                return json.dumps(response_meta)


        response_meta = { "response_op" : "error", "identity" : util.Identity.get_identity(), "error_message" : "Unrecognized operation" }
        return json.dumps(response_meta)


    def process_sql_statement(self, sql_statement, request):

        query_id = self.data_manager.get_query_id()

        query_start = time.time()

        logging.info("HttpReceiver: (%d) SQL received: %s", query_id, sql_statement.rstrip())

        if not sqlite3.complete_statement(sql_statement):

            # Try adding a semicolon at the end.
            sql_statement = sql_statement + ";"

            if not sqlite3.complete_statement(sql_statement):
                
                response_meta = { "response_op" : "error", 
                                  "error_message" : "Incomplete sql statement",
                                  "identity" : util.Identity.get_identity() }
                request.write(json.dumps(response_meta))
                request.finish()
                return

            # else it is now a complete statement

        d = self.data_manager.async_validate_and_route_query(sql_statement, query_id)

        d.addCallback(self.sql_complete_callback, query_id, query_start, request)

    def sql_complete_callback(self, result, query_id, query_start, request):
        """
        Called when a sql statement completes.
        """

        # To start just our identity.
        response_meta = { "identity" : util.Identity.get_identity() }

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
            logging.info("HttpReceiver: (%d) SQL error: %s", query_id, error_message)

            response_meta["response_op"] = "error"
            response_meta["error_message"] = error_message

        else:
            logging.info("HttpReceiver: (%d) SQL completed (%.02f seconds)", query_id, time.time() - query_start)

        # Send the response!

        request.write(json.dumps(response_meta))
        request.finish()


    def process_sql_statement_for_text(self, sql_statement, request):

        query_id = self.data_manager.get_query_id()

        query_start = time.time()

        logging.info("HttpReceiver: (%d) SQL received: %s", query_id, sql_statement.rstrip())

        if not sqlite3.complete_statement(sql_statement):

            # Try adding a semicolon at the end.
            sql_statement = sql_statement + ";"

            if not sqlite3.complete_statement(sql_statement):
                
                request.write("Incomplete sql statement")
                request.finish()
                return

            # else it is now a complete statement

        d = self.data_manager.async_validate_and_route_query(sql_statement, query_id)

        d.addCallback(self.sql_complete_callback_for_text, query_id, query_start, request)

    def sql_complete_callback_for_text(self, result, query_id, query_start, request):
        """
        Called when a sql statement completes.
        """

        retval        = result["retval"]
        data          = result["data"]          if "data"          in result else None
        error_message = result["error_message"] if "error_message" in result else ''
        max_widths    = result["max_widths"]    if "max_widths"    in result else None

        if retval == 0 and data != None:

            # I'm sure there is a better way but I'm just not thinking straight right now..
            class Outputter(object):
                @staticmethod
                def sendLine(text):
                    request.write(text + "\n")

            outputter = Outputter()

            util.pretty_print_table(outputter, data, max_widths=max_widths)

        if retval != 0:
            logging.info("HttpReceiver: (%d) SQL error: %s", query_id, error_message)
            request.write("SQL error: %s" % error_message)
        else:
            logging.info("HttpReceiver: (%d) SQL completed (%.02f seconds)", query_id, time.time() - query_start)

        request.finish()

