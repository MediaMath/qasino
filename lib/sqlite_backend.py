import sys
import os
import sqlite3
import re
import logging
import time

from util import Identity
import apsw_connection

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

    def __init__(self, filename, data_manager, archive_db_dir):
        self.data_manager = data_manager
        self.tables = {}
        self.connections = {}

        self.archive_db_dir = archive_db_dir
        self.main_thread = thread.get_ident()
        self.open_new_db(filename)

    def open_new_db(self, filename):
        self.filename = filename
        ##adbapi.noisy = True  # and add log.startLogging(sys.stdout) somewhere
        adbapi.connectionFactory = apsw_connection.ApswConnection
        self.dbpool = adbapi.ConnectionPool('apsw_connection', filename) ##, check_same_thread=False)
        self.dbpool.connectionFactory = apsw_connection.ApswConnection
       

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
            SqlConnections.shutdown(self.dbpool, self.filename, self.archive_db_dir)
        else:
            reactor.callLater(0.1, SqlConnections.shutdown, self.dbpool, self.filename, self.archive_db_dir)

    @staticmethod
    def shutdown(dbpool, filename, archive_db_dir):
        """
        Facilitates cleaning up filesystem objects after the last
        user of the connection pool goes away.
        """

        try:
            dbpool.close()
        except Exception as e:
            logging.info("SqlConnections.__del__: dbpool.close failed: %s", e)

        if filename != ':memory:':

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
                    pass  # ignore these
                    ##logging.info("SqlConnections: ERROR: Could not remove db file! %s", e)

    def do_select(self, txn, sql):

        try: 
            retval = txn.execute(sql)

        except Exception as e:
            return { "retval": 1, "error_message" : e }
            
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

        for row in txn.fetchall():

            saved_row = []

            # For each column.

            for column_index, cell in enumerate(row):

                # Convert all to strings and compute the length.

                if cell == None:
                    cell = ''
                else:
                    cell = str(cell)

                length = len(cell)

                if not max_widths.has_key(str(column_index)) or max_widths[str(column_index)] < length:
                    max_widths[str(column_index)] = length

                # Save the cell to the row for output later.

                saved_row.append(cell)

            # Save the row to the rows for output later.

            saved_rows.append(saved_row)


        data = { "column_names" : column_names,
                 "rows" : saved_rows }

        return { "retval" : 0, "error_message" : '', "data" : data, "max_widths" : max_widths }

    def get_schema(self, txn, table):

        schema = []

        txn.execute("pragma table_info( %s );" % table)

        for row in txn.fetchall():
            schema.append( [ str(row[1]), str(row[2]) ] )

        return schema


    def do_sql(self, txn, sql):
        """ Execute generic sql """

        result = None

        result = txn.execute(sql)

        return result


    def do_desc(self, txn, tablename):

        schema = self.get_schema(txn, tablename)

        if schema == None or len(schema) <= 0:
            return (1, "Table %s not found" % (tablename,), None)

        table = { "column_names" : [ "column_name", "column_type" ],
                  "rows" : schema }

        return (0, '', table)
        
    def do_update_table(self, txn, table, identity):

        tablename = table["tablename"]

        # Use only the first row for now.
        bind_values = [ x for x in table["rows"][0] ]

        # Make <column_name1>=?, <column_name2>=?, ...
        set_pairs = "=?, ".join( table["column_names"] ) + "=?"

        sql = "UPDATE %s SET %s WHERE identity=?" % (tablename, set_pairs)

        bind_values.append(identity)

        txn.execute(sql, bind_values)

        return txn.rowcount

    def do_insert_table(self, txn, table):

        tablename = table["tablename"]
        column_str = ", ".join( table["column_names"] )
        bind_str = ", ".join( [ "?" for x in table["column_names"] ] )

        sql = ''

        rowcount = 0

        for row_index, row in enumerate(table["rows"]):

            sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (tablename, column_str, bind_str)

            txn.execute(sql, row )

            rowcount += 1

        return rowcount


    def async_insert_info_table(self, db_generation_number, generation_start_epoch, generation_duration_s):
        """ 
        Adds a status table (qasino_server_info) to the database in each generation.
        """

        table = { "tablename" : "qasino_server_info",
                  "column_names" : [ "generation_number", "generation_duration_s", "generation_start_epoch" ],
                  "column_types" : [ "int", "int", "int" ],
                  "rows" : [ [ str(db_generation_number), generation_duration_s, generation_start_epoch ] ]
                  }

        return self.async_add_table_data(table, Identity.get_identity())


    def async_insert_tables_table(self):
        """ 
        Adds a table (qasino_server_tables) to the database with per table info.
        """

        rows = []

        for tablename, table_data in self.tables.items():
            
            rows.append( [ tablename, 
                           str(table_data["nr_rows"]),
                           str(table_data["updates"]),
                           table_data["last_update_epoch"] ] )

        # the chicken or the egg - how do we add ourselves?

        rows.append( [ "qasino_server_tables",
                       len(rows) + 1,
                       1,
                       time.time() ] )


        table = { "tablename" : "qasino_server_tables",
                  "column_names" : [ "tablename", "nr_rows", "nr_updates", "last_update_epoch" ],
                  "column_types" : [ "varchar", "int", "int", "int" ],
                  "rows" : rows
                }

        return self.async_add_table_data(table, Identity.get_identity())

    def async_insert_connections_table(self):
        """ 
        Adds a table (qasino_server_connections) to the database with per table info.
        """

        rows = []

        for connection, connection_data in self.connections.items():
            
            rows.append( [ connection, # identity
                           str(len(connection_data["tables"])),
                           connection_data["last_update_epoch"] ] )

        table = { "tablename" : "qasino_server_connections",
                  "column_names" : [ "identity", "nr_tables", "last_update_epoch" ],
                  "column_types" : [ "varchar", "int", "int" ],
                  "rows" : rows
                }

        return self.async_add_table_data(table, Identity.get_identity())


    def async_add_table_data(self, table, identity, **kwargs):
        """ 
        Initiate a adbapi async interaction to add a table to the backend, returns the deferred obj.
        """
        return self.dbpool.runInteraction(self.add_table_data, table, identity, **kwargs)

    def add_table_data(self, txn, table, identity, persist=False, update=False):
        """ 
        Add a table to the backend if it doesn't exist and insert the data.
        This is executed using adbapi in a thread pool.
        """

        tablename = table["tablename"]

        # Check if we need to save the table in case of persistence.

        self.data_manager.check_save_table(tablename, table, identity, persist, update)

        # Now check if we've already added the table.

        try:
            if not update and self.connections[identity]["tables"][tablename] > 0:
                logging.info("SqlConnections: '%s' has already sent an update for table '%s'", identity, tablename)
                return
            else:
                logging.info("SqlConnections: Update for table '%s' from '%s'", tablename, identity)
        except KeyError:
            # Ignore
            pass
            
        # Do we need to create the table in the db?

        did_create = False

        try:
            # Wrap the create, alter tables and inserts or updates in
            # a transaction... All or none please.

            txn.execute("BEGIN;")

            # Use this to detect if the table already exists - we
            # could just not do this and use the create table to
            # detect if a table is already there or not...

            schema = self.get_schema(txn, tablename)

            # Do we need to create the table?

            if schema == None or len(schema) <= 0:

                create_sql = "CREATE TABLE %s ( \n" % tablename

                columns = []
            
                for column_name, column_type in zip(table["column_names"], table["column_types"]):
                    columns.append( "%s %s DEFAULT NULL" % (column_name, column_type) )

                create_sql += ",\n".join( columns )

                create_sql += ")"

                logging.info("SqlConnections: Creating table '%s' from '%s'", tablename, identity)

                try:
                    result = self.do_sql(txn, create_sql)
                except Exception as e:
                    errorstr = str(e)

                    # Is this a "table already created" error?  Then its a race condition between checking if a table exists and actually creating it.
                    if "already exists" in errorstr:
                        # Don't need to log this as it is fairly common.
                        ##logging.info("SqlConnections: Warning: Create table race condition '%s' for %s: '%s' ... retrying", tablename, identity, errorstr)
                        raise CreateTableException("Create table race condition '%s' for %s: '%s'" % (tablename, identity, errorstr,))

                    else:
                        # Otherwise some kind of sql error, pass the exception up
                        raise e

                if result != None:
                    did_create = True

            else:
                logging.info("SqlConnections: Adding to table '%s' from '%s'", tablename, identity)


            # If we were not first and didn't create the table we need to check for merge.

            if not did_create:
                self.data_manager.table_merger.merge_table(txn, table, schema, self)

            # Now update the table - call do_update if this is an update type and we didn't create the initial table.

            if update and not did_create:
                rowcount = self.do_update_table(txn, table, identity)
            else:
                rowcount = self.do_insert_table(txn, table)

            txn.execute("COMMIT;")

        except apsw.BusyError as e:
            # This should only happen if we exceed the busy timeout.
            logging.info("SqlConnections: Warning: Lock contention for adding table '%s': %s for %s", tablename, str(e), identity)

            txn.execute("ROLLBACK;")

            # Place this request back on the event queue for a retry.
            self.async_add_table_data(table, identity, persist=persist, update=update)

            return 0

        except CreateTableException as e:
            # Message when raised

            txn.execute("ROLLBACK;")

            # Place this request back on the event queue for a retry.
            self.async_add_table_data(table, identity, persist=persist, update=update)

            return 0

        except Exception as e:
            logging.info("SqlConnections: ERROR: Failed to add table '%s': %s", tablename, e)

            txn.execute("ROLLBACK;")

            return 0

        now = time.time()

        nr_rows = len(table["rows"])

        # Keep track of how many updates a table has received.

        if tablename not in self.tables:
            self.tables[tablename] = { "updates" : 1, 
                                       "nr_rows" : rowcount,
                                       "last_update_epoch" : now }
        else:
            if not update:
                self.tables[tablename]["nr_rows"] += rowcount

            self.tables[tablename]["updates"] += 1
            self.tables[tablename]["last_update_epoch"] = now

        # Keep track of which "identities" have added to a table.

        if identity not in self.connections:

            self.connections[identity] = { 'tables' : { tablename : rowcount }, 
                                           'last_update_epoch' : now }
        else:
            self.connections[identity]["last_update_epoch"] = now

            if tablename not in self.connections[identity]["tables"]:
                self.connections[identity]["tables"][tablename] = rowcount
            elif not update:
                self.connections[identity]["tables"][tablename] += rowcount
            
        return 1



