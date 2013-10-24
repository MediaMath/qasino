
import logging
import simplejson as json
import re
import time
import sqlite3

from pprint import pprint
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.web import http

import util

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

        if 'op' in request.args:

            ## A simple name value update op.

            if request.args['op'][0] == "name_value_update":
                logging.info("HttpReceiver: Name value update.")

                if 'name' in request.args and 'value' in request.args:
                    
                    identity = request.args['identity'][0] if 'identity' in request.args else 'foo'
                    
                    # Parse the name into '<tablename>.<column>'
                    
                    name = request.args['name'][0]

                    m = re.search(r'^([\w_]+)\.([\w_]+)$', name)
                    if m == None:
                        logging.info("HttpReciever: Invalid name in name value update: '%s'", name)
                        return ''

                    tablename = m.group(1)
                    columnname = m.group(2)
                    value = request.args['value'][0]

                    table = { 'tablename' : tablename, 
                              'column_names' : [ 'identity', columnname ],
                              'column_types' : [ 'varchar', 'varchar' ],
                              'rows' : [ [ identity, value ] ]
                            }
                    self.data_manager.sql_backend_writer.async_add_table_data(table, identity, persist=True, update=True)

            ## Query op

            if request.args['op'][0] == "query":

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
                        return "Error processing sql: %s" % str(e)
                    else:
                        response_meta = { "response_op" : "error", "error_message" : "Error processing sql: %s" % str(e), "identity" : util.Identity.get_identity() }
                        return json.dumps(response_meta)

        return ''

    def render_POST(self, request):

        #pprint(request.__dict__)
        obj = dict()

        # The post body should be a json string.

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


        if 'op' in request.args:

            ## Table list op

            if request.args['op'][0] == "get_table_list":
                logging.info("HttpReceiver: Got request for table list.")
                response_meta = { "response_op" : "tables_list", "identity" : util.Identity.get_identity() }
                response_data = self.data_manager.get_table_list()
                return json.dumps(response_meta) + json.dumps(response_data)

            ## Add table data op

            if request.args['op'][0] == "add_table_data":
                logging.info("HttpReceiver: Add table data.")
                response_meta = { "response_op" : "ok", "identity" : util.Identity.get_identity() }
                try:
                    persist = True if "persist" in obj and obj["persist"] else False
                    if "static" in obj and obj["static"]:
                        self.data_manager.sql_backend_writer_static.async_add_table_data(obj["table"], obj["identity"], persist=persist, static=True)
                    else:
                        self.data_manager.sql_backend_writer.async_add_table_data(obj["table"], obj["identity"], persist=persist)
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

