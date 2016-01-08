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
import sqlite3
import re
import logging
import time
import string

from util import Identity, unicode_safe_str
import apsw_connection
import qasino_table

import apsw
from twisted.enterprise import adbapi
from twisted.internet import reactor

import thread

class CreateTableException(Exception):
    pass

class SqlConnections(object):
    """
    A pool of connections to a sql backend.
    Will be called either "sql_backend_writer" or "sql_backend_reader"
    by the data manager.
    """

    WRITER_INTERACTION = 0
    READER_INTERACTION = 1

    def __init__(self, filename, data_manager, archive_db_dir, thread_id, static_filename):
        self.data_manager = data_manager
        self.tables = {}
        self.connections = {}
        self.stats = {}

        self.archive_db_dir = archive_db_dir
        self.main_thread = thread_id
        self.open_new_db(filename, static_filename)

    def open_new_db(self, filename, static_filename):
        self.filename = filename
        self.static_filename = static_filename

        ##adbapi.noisy = True  # and add log.startLogging(sys.stdout) somewhere

        # since we're using apsw which is not technically DBAPI compliant 
        # I've created this wrapper class to make it act like one.
        adbapi.connectionFactory = apsw_connection.ApswConnection

        # if there is a static db ...
        def attach_static_db(conn):
            if static_filename != None and os.path.exists(static_filename):
                cur = conn.cursor()
                try:
                    result = cur.execute( "ATTACH DATABASE '%s' AS static;" % static_filename)
                except Exception as e:
                    logging.info("SqlConnections: ERROR Failed to attach static db '%s': %s", static_filename, str(e))

        # Open a connection pool for writing (one thread)...

        self.writer_dbpool = adbapi.ConnectionPool('apsw_connection', filename, 
                                                   cp_openfun=attach_static_db,
                                                   cp_min=1, cp_max=1)
                                                   
        self.writer_dbpool.connectionFactory = apsw_connection.ApswConnection

        # ...and one for reading (multiple threads).

        self.reader_dbpool = adbapi.ConnectionPool('apsw_connection', filename, 
                                                   cp_openfun=attach_static_db)

        self.reader_dbpool.connectionFactory = apsw_connection.ApswConnection


        # Preload list of tables already in db.

        self.reader_dbpool.runInteraction(self.preload_tables_list)
       

    def __del__(self):
        # This hack deals with when the condition where the last
        # reference to the sqlconnections object is held by a callback
        # and so the dtor is called from within a adbapi thread in the
        # thread pool.  If we are in one of these threads and call
        # dbpool.close it will throw an exception saying it can't join
        # the current thread - leaving a connection.  This may be a
        # non-issue and it only occurs with the first db created
        # (generation == 0) but all the same, detect when we are in a
        # different thread then the one we were contructed in and
        # schedule a call to shutdown (which must be static since this
        # is the dtor!) for later.

        if self.main_thread == thread.get_ident():
            SqlConnections.shutdown(self.writer_dbpool, self.filename, self.archive_db_dir)
            SqlConnections.shutdown(self.reader_dbpool, self.filename, self.archive_db_dir)
        else:
            reactor.callLater(0.1, SqlConnections.shutdown, self.writer_dbpool, self.filename, self.archive_db_dir)
            reactor.callLater(0.1, SqlConnections.shutdown, self.reader_dbpool, self.filename, self.archive_db_dir)

    @staticmethod
    def shutdown(dbpool, filename, archive_db_dir):
        """
        Facilitates cleaning up filesystem objects after the last
        user of the connection pool goes away.
        """

        try:
            dbpool.close()
        except Exception as e:
            logging.info("SqlConnections: dbpool.close failed in shutdown: %s", e)

        if filename != ':memory:' and os.path.exists(filename):

            if archive_db_dir:
                logging.info("SqlConnections: Archiving db file '%s' to '%s'", filename, archive_db_dir)
                try:
                    os.renames(filename, archive_db_dir)
                except Exception as e:
                    logging.info("SqlConnections: ERROR: Archive failed! %s", e)
            else:
                logging.info("SqlConnections: Removing db file '%s'", filename)
                try:
                    os.remove(filename)
                except Exception as e:
                    logging.info("SqlConnections: ERROR: Could not remove db file! %s", e)

    def do_select(self, txn, sql):

        self.stats['sql_received'] = self.stats.get('sql_received', 0) + 1

        try: 
            retval = txn.execute(sql)

        except Exception as e:

            self.stats['sql_errors'] = self.stats.get('sql_errors', 0) + 1

            return { "retval": 1, "error_message" : str(e) }

        # Find the max column with for each column.

        max_widths = {}

        # Get the column names.

        column_names = []

        try:
            for column_index, column_name in enumerate(txn.getdescription()):

                if not max_widths.has_key(str(column_index)) or max_widths[str(column_index)] < len(column_name[0]):
                    max_widths[str(column_index)] = len(column_name[0])
            
                column_names.append(column_name[0])

        except apsw.ExecutionCompleteError as e:
            # Ugh, getdescription fails if the query succeeds but returns no rows!  This is the message:
            #    "Can't get description for statements that have completed execution"

            # For now return a zero row empty table:

            data = { "column_names" : [ "query returned zero rows" ], "rows" : [ ] }

            return { "retval" : 0, "error_message" : '', "data" : data, "max_widths" : { "0" : 24 } }

        # Save the data here:

        saved_rows = []
        nr_rows = 0

        # For each row.

        # Under certain circustances (like 'select 1; abc') we'll get an exception in fetchall!
        try:
            for row in txn.fetchall():

                saved_row = []

                # For each column.

                for column_index, cell in enumerate(row):

                    # Convert all to strings and compute the length.

                    cell = unicode_safe_str(cell)

                    length = len(cell)

                    if not max_widths.has_key(str(column_index)) or max_widths[str(column_index)] < length:
                        max_widths[str(column_index)] = length

                    # Save the cell to the row for output later.

                    saved_row.append(cell)

                # Save the row to the rows for output later.

                saved_rows.append(saved_row)

        except ValueError as e:

            self.stats['sql_errors'] = self.stats.get('sql_errors', 0) + 1

            return { "retval": 1, "error_message" : str(e) }


        self.stats['sql_completed'] = self.stats.get('sql_completed', 0) + 1

        data = { "column_names" : column_names,
                 "rows" : saved_rows }

        return { "retval" : 0, "error_message" : '', "data" : data, "max_widths" : max_widths }

    def get_schema(self, txn, table):

        schema = []

        try:
            txn.execute("pragma table_info( '%s' );" % table)
        except Exception as e:
            logging.info("SqlConnections: Failed to get table_info for '%s'.", table)
            return schema

        for row in txn.fetchall():
            schema.append( [ str(row[1]), str(row[2]) ] )

        return schema


    def do_sql(self, txn, sql):
        """ Execute generic sql """
        return txn.execute(sql)


    def do_desc(self, txn, tablename):

        schema = self.get_schema(txn, tablename)

        if schema == None or len(schema) <= 0:
            return (1, "Table %s not found" % (tablename,), None)

        table = { "column_names" : [ "column_name", "column_type" ],
                  "rows" : schema }

        return (0, '', table)
        
    def do_update_table(self, txn, table, identity):

        self.stats['updates_received'] = self.stats.get('updates_received', 0) + 1

        key_cols_str = table.get_property('keycols')

        column_names = table.get_column_names()

        if key_cols_str == None:
            key_column_names = column_names
        else:
            key_column_names = string.split(key_cols_str, ';')

        tablename = table.get_tablename()
        index_name = tablename + "_unique_index_" + '_'.join(key_column_names)

        create_index_sql = "CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (%s)" % (index_name, tablename, ', '.join(key_column_names))
        #logging.info("DEBUG: would execute: %s", create_index_sql)

        try:
            txn.execute(create_index_sql)

        except Exception as e:
            logging.info("ERROR: Failed to create unique index for table update: '%s': ( %s )", str(e), create_index_sql)

            self.stats['update_errors'] = self.stats.get('update_errors', 0) + 1
            return

        sql = "INSERT OR REPLACE INTO %s (%s) VALUES (%s)" % (tablename, ', '.join(column_names), ', '.join([ '?' for x in column_names ]) )

        rowcount = 0

        for row in table.get_rows():

            #logging.info("DEBUG: would execute: %s with %s", sql, row)
            txn.execute(sql, row)

            rowcount += txn.getconnection().changes()

        self.stats['updates_completed'] = self.stats.get('updates_completed', 0) + 1

        return rowcount

    def do_insert_table(self, txn, table):
        """
        Insert table into the backend.  Use multi-row insert
        statements (supported in sqlite 3.7.11) for speed.
        """
        
        self.stats['inserts_received'] = self.stats.get('inserts_received', 0) + 1

        rowcount = 0

        base_query = "INSERT INTO %s (%s) VALUES " % (table.get_tablename(), ",".join( table.get_column_names() ) )

        nr_columns = len(table.get_column_names())
        max_nr_values = 400  # supposedly the max is 500..

        nr_values = 0
        bind_values = []

        one_row = '({})'.format(','.join(['?'] * nr_columns))

        sql = base_query

        for row_index, row in enumerate(table.get_rows()):

            if nr_values + nr_columns > max_nr_values:

                # execute this batch and start again.

                sql += ','.join([ one_row ] * (nr_values / nr_columns))

                #logging.info("DEBUG: hit limit: executing (values %d, rows %d): %s", nr_values, rowcount, sql)

                txn.execute(sql, bind_values)
                bind_values = []
                nr_values = 0
                sql = base_query

            nr_values += nr_columns

            # add this row to our bind values

            for value in row:
                bind_values.append(value)

            rowcount += 1

        # handle last batch
        if rowcount > 0:
            sql += ','.join([ one_row ] * (nr_values / nr_columns))

            #logging.info("DEBUG: final: executing (values %d, rows %d): %s", nr_values, rowcount, sql)

            txn.execute(sql, bind_values)

        self.stats['inserts_completed'] = self.stats.get('inserts_completed', 0) + 1

        return rowcount

    def run_interaction(self, interaction_type, callback, *args, **kwargs):
        """ 
        Initiate a adbapi async interaction run a generic function, returns the deferred obj.
        """
        if interaction_type == SqlConnections.WRITER_INTERACTION:
            return self.writer_dbpool.runInteraction(callback, *args, **kwargs)

        elif interaction_type == SqlConnections.READER_INTERACTION:
            return self.reader_dbpool.runInteraction(callback, *args, **kwargs)

        return

    def insert_info_table(self, txn, db_generation_number, generation_start_epoch, generation_duration_s):
        """ 
        Adds a status table (qasino_server_info) to the database in each generation.
        """
        table = qasino_table.QasinoTable("qasino_server_info")
        table.add_column("generation_number",      "int")
        table.add_column("generation_duration_s",  "int")
        table.add_column("generation_start_epoch", "int")

        table.add_row( [ str(db_generation_number), generation_duration_s, generation_start_epoch ] )

        return self.add_table_data(txn, table, Identity.get_identity())

    def insert_sql_stats_table(self, txn, sql_backend_reader):
        """ 
        Adds a status table (qasino_server_sql_stats) to the database in each generation.
        Note we are actually saving stats from the "reader" backend because that is where 
        sql stats are logged.
        """
        table = qasino_table.QasinoTable("qasino_server_sql_stats")
        table.add_column("sql_received",  "int")
        table.add_column("sql_completed", "int")
        table.add_column("sql_errors",    "int")

        table.add_row( [ sql_backend_reader.stats.get('sql_received', 0),
                         sql_backend_reader.stats.get('sql_completed', 0),
                         sql_backend_reader.stats.get('sql_errors', 0) ] )

        return self.add_table_data(txn, table, Identity.get_identity())

    def insert_update_stats_table(self, txn):
        """ 
        Adds a status table (qasino_server_update_stats) to the database in each generation.
        """
        table = qasino_table.QasinoTable("qasino_server_update_stats")
        table.add_column("updates_received",  "int")
        table.add_column("updates_completed", "int")
        table.add_column("update_errors",     "int")
        table.add_column("inserts_received",  "int")
        table.add_column("inserts_completed", "int")

        table.add_row( [ self.stats.get('updates_received', 0), 
                         self.stats.get('updsates_completed', 0),
                         self.stats.get('update_errors', 0),
                         self.stats.get('inserts_received', 0), 
                         self.stats.get('inserts_completed', 0) ] )

        return self.add_table_data(txn, table, Identity.get_identity())

    def add_tables_table_rows(self, table):

        for tablename, table_data in self.tables.items():
            
            table.add_row( [ tablename, 
                             str(table_data["nr_rows"]),
                             str(table_data["updates"]),
                             table_data["last_update_epoch"],
                             0 if self.static_filename != None else 1 ]
                           )

    def preload_tables_list(self, txn):
        """ 
        This is used upon initial open of the db to get an actual list
        of tables from the db.
        """

        self.do_sql(txn, "SELECT tbl_name FROM sqlite_master WHERE type = 'table' and tbl_name NOT LIKE 'sqlite_%'")

        rows = txn.fetchall()

        for row in rows:

            tablename = row[0]

            try:
                self.do_sql(txn, "SELECT count(*) FROM %s;" % tablename)

                count = txn.fetchall()

                self.update_table_stats(tablename, count[0][0] if count[0][0] != None and count[0][0] > 0 else -1)
            except:
                logging.info("SqlConnections: Failed to get number of rows for table '%s': %s", tablename, str(e))
                pass

    def insert_connections_table(self, txn):
        """ 
        Adds a table (qasino_server_connections) to the database with per table info.
        """

        table = qasino_table.QasinoTable("qasino_server_connections")
        table.add_column("identity",          "varchar")
        table.add_column("nr_tables",         "int")
        table.add_column("last_update_epoch", "int")

        for connection, connection_data in self.connections.items():
            
            table.add_row( [ connection, # identity
                             str(len(connection_data["tables"])),
                             connection_data["last_update_epoch"] ] )

        return self.add_table_data(txn, table, Identity.get_identity())

    def add_views(self, txn, views):
        """
        Add all views to the backend.  This should not take a long time.
        """
        for viewname, viewdata in views.iteritems():

            logging.info("SqlConnections: Adding view '%s'", viewname)

            try:
                self.do_sql(txn, viewdata['view'])
                viewdata['loaded'] = True
                viewdata['error'] = ''
            except Exception as e:
                logging.info("SqlConnections: ERROR: Failed to add view '%s': %s", viewname, str(e))
                viewdata['loaded'] = False
                viewdata['error'] = str(e)

    def insert_views_table(self, txn, views):
        """ 
        Adds a table (qasino_server_connections) to the database with per table info.
        """

        table = qasino_table.QasinoTable("qasino_server_views")
        table.add_column("viewname", "varchar")
        table.add_column("loaded",   "int")
        table.add_column("errormsg", "varchar")
        table.add_column("view",     "varchar")

        for viewname, viewdata in views.iteritems():
            table.add_row( [ viewname, str(int(viewdata['loaded'])), str(viewdata['error']), viewdata['view'] ] )

        return self.add_table_data(txn, table, Identity.get_identity())

    def update_table_stats(self, tablename, nr_rows, identity=Identity.get_identity(), now=time.time(), sum=False):

        # Keep track of how many updates a table has received.

        if tablename not in self.tables:
            self.tables[tablename] = { "updates" : 1, 
                                       "nr_rows" : nr_rows,
                                       "last_update_epoch" : now }
        else:
            if sum:
                self.tables[tablename]["nr_rows"] += nr_rows

            self.tables[tablename]["updates"] += 1
            self.tables[tablename]["last_update_epoch"] = now


        # Keep track of which "identities" have added to a table.

        if identity not in self.connections:

            self.connections[identity] = { 'tables' : { tablename : nr_rows }, 
                                           'last_update_epoch' : now }
        else:
            self.connections[identity]["last_update_epoch"] = now

            if sum and tablename in self.connections[identity]["tables"]:
                self.connections[identity]["tables"][tablename] += nr_rows
            else:
                self.connections[identity]["tables"][tablename] = nr_rows

    def async_add_table_data(self, *args, **kwargs):
        """
        Initiate a adbapi async interaction to add a table to the backend, returns the deferred obj.
        """
        return self.writer_dbpool.runInteraction(self.add_table_data, *args, **kwargs)

    def add_table_data(self, txn, table, identity):
        """ 
        Add a table to the backend if it doesn't exist and insert the data.
        This is executed using adbapi in a thread pool.
        """

        start_time = time.time()

        # For when we hit lock contention - only retry so many times.

        table.init_retry(5)

        tablename = table.get_tablename()

        if tablename is None:
            logging.info("SqlConnections: Unknown tablename from '%s'", identity)
            return

        update = table.get_property('update')
        static = table.get_property('static')
        persist = table.get_property('persist')

        # Check if we need to save the table in case of persistence.

        self.data_manager.check_save_table(table, identity)

        # Now check if we've already added the table.

        try:
            if not update and not static and self.connections[identity]["tables"][tablename] > 0:
                logging.info("SqlConnections: '%s' has already sent an update or is persisted for table '%s'", identity, tablename)
                return
        except KeyError:
            # Ignore
            pass

        try:
            # Wrap the create, alter tables and inserts or updates in
            # a transaction... All or none please.

            txn.execute("BEGIN;")

            # Use this to detect if the table already exists - we
            # could just not do this and use the create table to
            # detect if a table is already there or not but a error on
            # the create table would mean having to restart the
            # transaction and require some weird retry logic..

            create_sql = "CREATE TABLE IF NOT EXISTS '%s' ( \n" % tablename

            columns = []
            
            for column_name, column_type in table.zip_columns():
                columns.append( "'%s' %s DEFAULT NULL" % (column_name, column_type) )

            create_sql += ",\n".join( columns )

            create_sql += ")"

            try:
                result = self.do_sql(txn, create_sql)
            except Exception as e:
                errorstr = str(e)

                # Is this a "table already exists" error?  Then its a race condition between checking if a table exists and actually creating it.
                # This should never happen now - we are using CREATE IF NOT EXISTS ...
                if "already exists" in errorstr:
                    # Don't need to log this as it is fairly common.
                    ##logging.info("SqlConnections: Warning: Create table race condition '%s' for %s: '%s' ... retrying", tablename, identity, errorstr)
                    raise CreateTableException("Create table race condition '%s' for %s: '%s'" % (tablename, identity, errorstr,))
                
                else:
                    # Otherwise some kind of sql error, pass the exception up
                    raise e


            # Merge the schemas of what might be already there and our update.
            schema = self.get_schema(txn, tablename)

            self.data_manager.table_merger.merge_table(txn, table, schema, self)

            # Now update the table.
            # Truncate and re-insert if this is static.
            # Call do_update if this is an update type.
            # Otherwise we insert.

            if static:
                txn.execute("DELETE FROM '%s';" % tablename)
                rowcount = self.do_insert_table(txn, table)

            else:
                if update:
                    rowcount = self.do_update_table(txn, table, identity)
                else:
                    rowcount = self.do_insert_table(txn, table)

            txn.execute("COMMIT;")

        except apsw.BusyError as e:
            # This should only happen if we exceed the busy timeout.
            logging.info("SqlConnections: Warning: Lock contention for adding table '%s': %s for %s", tablename, str(e), identity)

            try:
                txn.execute("ROLLBACK;")
            except:
                pass

            # Should we try again?
            if table.test_retry():
                logging.info("SqlConnections: ERROR: Retries failed for table '%s'", tablename)
                return 0

            # Place this request back on the event queue for a retry.
            self.async_add_table_data(table, identity)

            return 0

        except CreateTableException as e:
            # Message when raised

            try:
                txn.execute("ROLLBACK;")
            except:
                pass

            # Should we try again?
            if table.test_retry():
                logging.info("SqlConnections: ERROR: Retries failed for table '%s'", tablename)
                return 0

            # Place this request back on the event queue for a retry.
            self.async_add_table_data(table, identity)

            return 0

        except Exception as e:
            logging.info("SqlConnections: ERROR: Failed to add table '%s': %s", tablename, e)

            try:
                txn.execute("ROLLBACK;")
            except:
                pass

            return 0

        now = time.time()

        nr_rows = table.get_nr_rows()

        properties_str = ''
        if static: properties_str += ' static'
        if update: properties_str += ' update'
        if persist: properties_str += ' persist'

        logging.info("SqlConnections: New data%s for table '%s' %d rows from '%s' (%.02f seconds)", 
                     properties_str, tablename, rowcount, identity, now - start_time)

        # Update some informational stats used for making the tables
        # and connections tables.

        self.update_table_stats(tablename, rowcount, identity=identity,
                                sum=not update and not static, now=now)
            
        return 1



