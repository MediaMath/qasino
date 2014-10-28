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

from jinja2 import Environment, FileSystemLoader

class UIResource(Resource):

    isLeaf = True

    def __init__(self, templates_dir, data_manager):
        self.data_manager = data_manager
        self.jinja = Environment(loader=FileSystemLoader(templates_dir))

    def render_page(self, page_name, request, context={}):
        
        request.setHeader("Content-Type", "text/html; charset=utf-8")

        ctx = { 'page' : page_name,
                'params' : request.args }

        # Do it this way so input can completely override.
        for k, v in context.iteritems():
            ctx[k] = v

        template = self.jinja.get_template(page_name + ".html.j2")

        output = template.render(ctx)

        return output.encode('utf-8')


    def render_page_with_sql(self, page_name, sql, request, context={}):

        request.setHeader("Content-Type", "text/html; charset=utf-8")

        query_id = self.data_manager.get_query_id()
        query_start = time.time()

        d = self.data_manager.async_validate_and_route_query(sql, query_id)

        d.addCallback(self.sql_complete_callback, page_name, query_id, query_start, request, context=context)

        return NOT_DONE_YET
    
    def sql_complete_callback(self, result, page_name, query_id, query_start, request, context={}):

        data = result["data"] if "data" in result else None

        ctx = { 'page' : page_name,
                'rows' : [],
                'column_names' : [],
                'params' : request.args,
                }

        # Do it this way so input can completely override.
        for k, v in context.iteritems():
            ctx[k] = v


        template = self.jinja.get_template(page_name + ".html.j2")

        err_msg = str(result.get('error_message', ''))

        if data is None and err_msg == '':

            ctx['error_message'] = 'No data!'

        elif err_msg != '':

            ctx['error_message'] = err_msg

        elif "rows" in data and "column_names" in data:

            ctx['rows'] = data['rows']
            ctx['column_names'] = data['column_names']

        else:
            ctx['error_message'] = 'No row or column data in result!'

        output = template.render(ctx)
        request.write( output.encode('utf-8') )

        request.finish()


class UIResourceTables(UIResource):

    def render_GET(self, request):

        sql = "select tablename, nr_rows, nr_updates, datetime(last_update_epoch, 'unixepoch') last_update, static from qasino_server_tables order by tablename;"

        name_map = { 'tablename' : 'Tablename',
                     'nr_rows' : 'Nr Rows',
                     'nr_updates' : 'Nr Updates',
                     'last_update' : 'Last Update',
                     'static' : 'Static',
                     }

        return self.render_page_with_sql("tables", sql, request, context={ 'pretty_column_names_map' : name_map,
                                                                           'links' : { 'tablename' : '/desc?tablename=%s' } })


class UIResourceDesc(UIResource):

    def render_GET(self, request):

        if 'tablename' in request.args:
            
            sql = "desc {};".format(request.args['tablename'][0].replace(" ", ""))

            name_map = { 'column_name' : 'Column Name',
                         'column_type' : 'Column Type',
                         }

            return self.render_page_with_sql("desc", sql, request, context={ 'pretty_column_names_map' : name_map })
        else:

            return self.render_page("desc", request)


class UIResourceQuery(UIResource):

    def render_GET(self, request):

        if 'sql' in request.args:
            
            m = re.search(r"^\s*select\s+", request.args['sql'][0], flags=re.IGNORECASE)
            if m == None:
                
                return self.render_page("query", request, 
                                        context={ 'error_message' : 'Please use only SELECT statements.' })

            sql = "{};".format(request.args['sql'][0])

            return self.render_page_with_sql("query", sql, request)
        else:

            return self.render_page("query", request)

