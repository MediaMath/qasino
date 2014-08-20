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

import sqlite_backend as sql_backend

import table_merger
import util
import qasino_table

import logging
import time
import re
import yaml
import sys
import thread

from twisted.internet import threads
from twisted.internet import task

class DataManager(object):

    def __init__(self, use_dbfile, db_dir=None, signal_channel=None, archive_db_dir=None, 
                 generation_duration_s=30):

        self.saved_tables = {}
        self.query_id = 0
        self.views = {}
        self.thread_id = thread.get_ident()
        self.stats = {}

        self.generation_duration_s = generation_duration_s
        self.signal_channel = signal_channel
        self.archive_db_dir = archive_db_dir
        self.static_db_filepath = db_dir + '/qasino_table_store_static.db'

        # Start with zero because we'll call rotate_dbs instantly below.
        self.db_generation_number = 0

        # use_dbfile can be:
        #   'memory- -> use in memory db
        #   /%d/ -> use the provided template filename
        #   ! /%d/ -> use the filename (same db every generation)

        self.one_db = False
        self.db_name = use_dbfile

        if use_dbfile == None:
            self.db_name = "qasino_table_store_%d.db"

        elif use_dbfile == "memory":
            self.db_name = ":memory:"
            self.one_db = True

        elif use_dbfile.find('%d') == -1:
            self.one_db = True

        # Initialize some things

        self.table_merger = table_merger.TableMerger(self)

        # Add db_dir path

        if db_dir != None and self.db_name != ":memory:":
            self.db_name = db_dir + '/' + self.db_name

        # Open the writer backend db.

        db_file_name = self.db_name

        if not self.one_db:
            db_file_name = self.db_name % self.db_generation_number

        self.sql_backend_reader = None
        self.sql_backend_writer = sql_backend.SqlConnections(db_file_name, 
                                                             self, 
                                                             self.archive_db_dir, 
                                                             self.thread_id, 
                                                             self.static_db_filepath)

        self.sql_backend_writer_static = sql_backend.SqlConnections(self.static_db_filepath, 
                                                                    self, 
                                                                    self.archive_db_dir, 
                                                                    self.thread_id, 
                                                                    None)
                                                                    

        # Make the data manager db rotation run at fixed intervals.
        # This will also immediately make the call which will make the
        # writer we just opened the reader and to open a new writer.

        self.rotate_task = task.LoopingCall(self.async_rotate_dbs)
        self.rotate_task.start(self.generation_duration_s)


    def read_views(self, filename):

        # Reset views
        self.views = {}

        try:
            fh = open(filename, "r")
        except Exception as e:
            logging.info("Failed to open views file '%s': %s", filename, e)
            return

        try:
            view_conf_obj = yaml.load(fh)
        except Exception as e:
            logging.info("Failed to parse view conf yaml file '%s': %s", filename, e)
            return

        for view in view_conf_obj:

            try:
                viewname = view["viewname"]
                view = view["view"]
                self.views[viewname] = { 'view' : view, 'loaded' : False, 'error' : '' }
            except Exception as e:
                logging.info("Failure getting view '%s': %s", view["viewname"] if "viewname" in view else 'unknown', e)

    def get_query_id(self):
        self.query_id += 1
        return self.query_id

    def shutdown(self):
        self.rotate_task = None
        self.sql_backend_reader = None
        self.sql_backend_writer = None


    def async_validate_and_route_query(self, sql, query_id, use_write_db=False):
        if use_write_db:
            return self.sql_backend_writer.run_interaction(sql_backend.SqlConnections.WRITER_INTERACTION, 
                                                           self.validate_and_route_query, sql, query_id, self.sql_backend_writer)
        else:
            return self.sql_backend_reader.run_interaction(sql_backend.SqlConnections.READER_INTERACTION,
                                                           self.validate_and_route_query, sql, query_id, self.sql_backend_reader)


    def validate_and_route_query(self, txn, sql, query_id, sql_backend):

        m = re.search(r"^\s*select\s+", sql, flags=re.IGNORECASE)
        if m == None:

            # Process a non-select statement.

            return self.process_non_select(txn, sql, query_id, sql_backend)

        # Process a select statement.

        return sql_backend.do_select(txn, sql)


    def process_non_select(self, txn, sql, query_id, sql_backend):
        """
        Called for non-select statements like show tables and desc.
        """

        # DESC?

        m = re.search(r"^\s*desc\s+(\S+)\s*;$", sql, flags=re.IGNORECASE)

        if m != None:
            (retval, error_message, table) = sql_backend.do_desc(txn, m.group(1))

            result = { "retval" : retval }
            if error_message:
                result["error_message"] = error_message
            if table:
                result["data"] = table

            return result

        # DESC VIEW?

        m = re.search(r"^\s*desc\s+view\s+(\S+)\s*;$", sql, flags=re.IGNORECASE)
        if m != None:
            return sql_backend.do_select(txn, "SELECT view FROM qasino_server_views WHERE viewname = '%s';" % m.group(1))
            
        # SHOW tables?

        m = re.search(r"^\s*show\s+tables\s*;$", sql, flags=re.IGNORECASE)
        if m != None:
            return sql_backend.do_select(txn, "SELECT *, strftime('%Y-%m-%d %H:%M:%f UTC', last_update_epoch, 'unixepoch') last_update_datetime FROM qasino_server_tables ORDER BY tablename;")

        # SHOW tables with LIKE?

        m = re.search(r"^\s*show\s+tables\s+like\s+('\S+')\s*;$", sql, flags=re.IGNORECASE)
        if m != None:
            return sql_backend.do_select(txn, "SELECT *, strftime('%Y-%m-%d %H:%M:%f UTC', last_update_epoch, 'unixepoch') last_update_datetime FROM qasino_server_tables WHERE tablename LIKE {} ORDER BY tablename;".format(m.group(1)) )

        # SHOW connections?

        m = re.search(r"^\s*show\s+connections\s*;$", sql, flags=re.IGNORECASE)
        if m != None:
            return sql_backend.do_select(txn, "SELECT *, strftime('%Y-%m-%d %H:%M:%f UTC', last_update_epoch, 'unixepoch') last_update_datetime FROM qasino_server_connections ORDER BY identity;")

        # SHOW info?

        m = re.search(r"^\s*show\s+info\s*;$", sql, flags=re.IGNORECASE)
        if m != None:
            return sql_backend.do_select(txn, "SELECT *, strftime('%Y-%m-%d %H:%M:%f UTC', generation_start_epoch, 'unixepoch') generation_start_datetime FROM qasino_server_info;")

        # SHOW views?

        m = re.search(r"^\s*show\s+views\s*;$", sql, flags=re.IGNORECASE)
        if m != None:
            return sql_backend.do_select(txn, "SELECT viewname, loaded, errormsg FROM qasino_server_views ORDER BY viewname;")

        # Exit?

        m = re.search(r"^\s*(quit|logout|exit)\s*;$", sql, flags=re.IGNORECASE)
        if m != None:
            return { "retval" : 0, "error_message" : "Bye!" }


        return { "retval" : 1, "error_message" : "ERROR: Unrecognized statement: %s" % sql }


    def get_table_list(self):

        return self.sql_backend_reader.tables

    def insert_tables_table(self, txn, sql_backend_writer, sql_backend_writer_static):

        table = qasino_table.QasinoTable("qasino_server_tables")
        table.add_column("tablename",         "varchar")
        table.add_column("nr_rows",           "int")
        table.add_column("nr_updates",        "int")
        table.add_column("last_update_epoch", "int")
        table.add_column("static",            "int")

        sql_backend_writer.add_tables_table_rows(table)

        sql_backend_writer_static.add_tables_table_rows(table)

        # the chicken or the egg - how do we add ourselves?

        table.add_row( [ "qasino_server_tables",
                         table.get_nr_rows() + 1,
                         1,
                         time.time(), 
                         0 ] )

        return sql_backend_writer.add_table_data(txn, table, util.Identity.get_identity())
        

    # This hack insures all the internal tables are inserted
    # using the same sql_backend_writer and makes sure that the
    # "tables" table is called last (after all the other internal
    # tables are added).

    def insert_internal_tables(self, txn, sql_backend_writer, sql_backend_reader, db_generation_number, time, generation_duration_s, views):

        sql_backend_writer.insert_info_table(txn, db_generation_number, time, generation_duration_s)

        sql_backend_writer.insert_connections_table(txn)

        if sql_backend_reader != None:
            sql_backend_writer.insert_sql_stats_table(txn, sql_backend_reader)

        sql_backend_writer.insert_update_stats_table(txn)

        # this should be second last so views can be created of any tables above.
        # this means though that you can not create views of any tables below.
        sql_backend_writer.add_views(txn, views)

        sql_backend_writer.insert_views_table(txn, views)

        # this should be last to include all the above tables

        self.insert_tables_table(txn, sql_backend_writer, self.sql_backend_writer_static)

    def async_rotate_dbs(self):
        """
        Kick off the rotate in a sqlconnection context because we have
        some internal tables and views to add before we rotate dbs.
        """
        
        self.sql_backend_writer.run_interaction(sql_backend.SqlConnections.WRITER_INTERACTION, self.rotate_dbs)

    def rotate_dbs(self, txn):
        """ 
        Make the db being written to be the reader db.
        Open a new writer db for all new updates.
        """

        logging.info("**** DataManager: Starting generation %d", self.db_generation_number)

        # Before making the write db the read db, 
        # add various internal info tables and views.


        self.insert_internal_tables(txn, 
                                    self.sql_backend_writer, 
                                    self.sql_backend_reader,
                                    self.db_generation_number, 
                                    time.time(), 
                                    self.generation_duration_s, 
                                    self.views)

        # Increment the generation number.

        self.db_generation_number = int(time.time())

        # Set the writer to a new db

        save_sql_backend_writer = self.sql_backend_writer

        # If specified put the generation number in the db name.

        db_file_name = self.db_name

        if not self.one_db:
                db_file_name = self.db_name % self.db_generation_number

        self.sql_backend_writer = sql_backend.SqlConnections(db_file_name, 
                                                             self, 
                                                             self.archive_db_dir, 
                                                             self.thread_id, 
                                                             self.static_db_filepath)

        # Set the reader to what was the writer
        # Note the reader will (should) be deconstructed here.

        self.sql_backend_reader = save_sql_backend_writer

        # Load saved tables.

        self.async_add_saved_tables()

        # Lastly blast out the generation number.

        if self.signal_channel != None:
            self.signal_channel.send_generation_signal(self.db_generation_number, self.generation_duration_s)


    def check_save_table(self, table, identity):

        tablename = table.get_tablename()

        key = tablename + identity

        if table.get_property('persist'):
            self.saved_tables[key] = { "table" : table, "tablename" : tablename, "identity" : identity }
        else:
            # Be sure to remove a table that is no longer persisting.
            if key in self.saved_tables:
                del self.saved_tables[key]


    def async_add_saved_tables(self):

        for key, table_data in self.saved_tables.iteritems():

            logging.info("DataManager: Adding saved table '%s' from '%s'", table_data["tablename"], table_data["identity"])

            self.sql_backend_writer.async_add_table_data(table_data["table"], table_data["identity"])
