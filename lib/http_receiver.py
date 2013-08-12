
from pprint import pprint
from twisted.web.resource import Resource
import logging
import json
import re

from util import Identity


class HttpReceiver(Resource):

    def __init__(self, data_manager):
        self.data_manager = data_manager

    def render_GET(self, request):
        print "GOT ", request
        if 'op' in request.args:
            if request.args['op'][0] == "name_value_update":
                logging.info("HttpReceiver: Name value update.")

                if 'name' in request.args and 'value' in request.args:
                    
                    print request
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

        return ''

    def render_POST(self, request):
        #pprint(request.__dict__)

        newdata = request.content.getvalue()
        #print "Received POST:"
        #print newdata

        if 'op' in request.args:
            if request.args['op'][0] == "get_table_list":
                logging.info("HttpReceiver: Got request for table list.")
                response_meta = { "response_op" : "tables_list", "identity" : Identity.get_identity() }
                response_data = self.data_manager.get_table_list()
                return json.dumps(response_meta) + json.dumps(response_data)
            if request.args['op'][0] == "add_table_data":
                logging.info("HttpReceiver: Add table data.")
                response_meta = { "response_op" : "ok", "identity" : Identity.get_identity() }
                persist = True if "persist" in obj and obj["persist"] else False
                try:
                    obj = json.loads(newdata)
                    self.data_manager.sql_backend_writer.async_add_table_data(obj["table"], obj["identity"], persist=persist)
                except Exception as e:
                    response_meta = { "response_op" : "error", "identity" : Identity.get_identity(), "error_message" : str(e) }
                
                return json.dumps(response_meta)

        response_meta = { "response_op" : "error", "identity" : Identity.get_identity(), "error_message" : "Unrecognized operation" }
        return json.dumps(response_meta)
