import sys
import os
import sqlite3
import re
import logging
import time

from util import Identity

from twisted.enterprise import adbapi


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
        self.open_new_db(filename)

    def open_new_db(self, filename):
        self.filename = filename
        self.dbpool = adbapi.ConnectionPool('sqlite3', filename, check_same_thread=False)

    def __del__(self):
        """
        Facilitates cleaning up filesystem objects after the last
        user of the connection pool goes away.
        """

        if self.filename != ':memory:':

            if self.archive_db_dir:
                logging.info("Archiving db file '%s' to '%s'", self.filename, self.archive_db_dir)
                try:
                    os.renames(self.filename, self.archive_db_dir)
                except Exception as e:
                    logging.info("ERROR: Archive failed! %s", e)
            else:
                logging.info("Removing db file '%s'", self.filename)
                try:
                    os.remove(self.filename)
                except Exception as e:
                    pass  # ignore these
                    ##logging.info("ERROR: Could not remove db file! %s", e)

#    def get_sql_receiver(self, factory, connection_id):
#        return SqlReceiver(factory, connection_id)

    def do_select(self, txn, sql):

        try: 
            txn.execute(sql)

        except sqlite3.OperationalError as e:
            return { "retval": 1, "error_message" : e }

        except Exception as e:
            return { "retval": 1, "error_message" : e }
            
        # Find the max column with for each column.

        max_widths = {}

        # Get the column names.

        column_names = []

        for column_index, column_name in enumerate(txn.description):

            if not max_widths.has_key(str(column_index)) or max_widths[str(column_index)] < len(column_name[0]):
                max_widths[str(column_index)] = len(column_name[0])
            
            column_names.append(column_name[0])

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

        try: 
            txn.execute("pragma table_info(%s);" % table)

            for row in txn.fetchall():
                schema.append( [ str(row[1]), str(row[2]) ] )
        except:
            pass

        return schema


    def do_sql(self, txn, sql):
        """ Execute generic sql """

        result = None

        try:
            result = txn.execute(sql)
        except Exception as e:
            logging.info("Warning: sql failure '%s' for:\n %s", e, sql)

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

        try:
            txn.execute(sql, bind_values)
        except Exception as e:
            logging.info("ERROR: Failed to update table '%s' (%s): %s", tablename, sql, e)

        return txn.rowcount

    def do_insert_table(self, txn, table):

        tablename = table["tablename"]
        column_str = ", ".join( table["column_names"] )
        bind_str = ", ".join( [ "?" for x in table["column_names"] ] )

        # TODO: more efficient inserts
        # TODO: transactional (all or none)

        rowcount = 0

        for row_index, row in enumerate(table["rows"]):

            sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (tablename, column_str, bind_str)

            try:
                txn.execute(sql, row )
            except Exception as e:
                logging.info("ERROR: Failed to insert into table '%s' (%s): %s", tablename, sql, e)
                break

            rowcount += txn.rowcount

        return rowcount


    def async_insert_status_table(self, db_generation_number):
        """ 
        Adds a status table (qasino_server_info) to the database in each generation.
        """

        table = { "tablename" : "qasino_server_info",
                  "column_names" : [ "generation_number", "generation_start_time" ],
                  "column_types" : [ "int", "varchar" ],
                  "rows" : [ [ str(db_generation_number), time.strftime("%Y-%m-%d %H:%M:%S GMT", time.gmtime()) ] ]
                  }

        return self.async_add_table_data(table, Identity.get_identity())


    def async_insert_tables_table(self):
        """ 
        Adds a table (qasino_table_info) to the database with per table info.
        """

        rows = []

        for tablename, table_data in self.tables.items():
            
            rows.append( [ tablename, 
                           str(table_data["nr_rows"]),
                           str(table_data["updates"]),
                           table_data["last_update_dt"] ] )

        # chicken and the egg - how do we add ourselves?

        rows.append( [ "qasino_table_info",
                       len(rows) + 1,
                       1,
                       time.strftime("%Y-%m-%d %H:%M:%S GMT", time.gmtime()) ] )


        table = { "tablename" : "qasino_table_info",
                  "column_names" : [ "tablename", "nr_rows", "nr_updates", "last_update_dt" ],
                  "column_types" : [ "varchar", "int", "int", "varchar" ],
                  "rows" : rows
                  }

        return self.async_add_table_data(table, Identity.get_identity())

    def async_insert_connections_table(self):
        """ 
        Adds a table (qasino_connection_info) to the database with per table info.
        """

        rows = []

        for connection, connection_data in self.connections.items():
            
            rows.append( [ connection, # identity
                           str(len(connection_data["tables"])),
                           connection_data["last_update_dt"] ] )

        table = { "tablename" : "qasino_connection_info",
                  "column_names" : [ "identity", "nr_tables", "last_update_dt" ],
                  "column_types" : [ "varchar", "int", "varchar" ],
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
                logging.info("DataManager: '%s' has already sent an update for table '%s'", identity, tablename)
                return
            else:
                logging.info("DataManager: Update for table '%s' from '%s'", tablename, identity)
        except KeyError:
            # Ignore
            pass
            
        # Do we need to create the table in the db?

        did_create = False

        schema = self.get_schema(txn, tablename)

        if schema == None or len(schema) <= 0:
            create_sql = "CREATE TABLE %s ( \n" % tablename

            columns = []
            
            for column_name, column_type in zip(table["column_names"], table["column_types"]):
                columns.append( "%s %s DEFAULT NULL" % (column_name, column_type) )

            create_sql += ",\n".join( columns )

            create_sql += ")"

            logging.info("DataManager: Creating table '%s' from '%s'", tablename, identity)

            result = self.do_sql(txn, create_sql)
            if result != None:
                did_create = True

        else:
            logging.info("DataManager: Adding to table '%s' from '%s'", tablename, identity)


        # If we were not first and didn't create the table we need to check for merge.

        if not did_create:
            self.data_manager.table_merger.merge_table(txn, table, schema, self)

        # Now update the table - call do_update if this is an update type and we didn't create the initial table.

        if update and not did_create:
            rowcount = self.do_update_table(txn, table, identity)
        else:
            rowcount = self.do_insert_table(txn, table)


        now = time.strftime("%Y-%m-%d %H:%M:%S GMT", time.gmtime())

        nr_rows = len(table["rows"])

        # Keep track of how many updates a table has received.

        if tablename not in self.tables:
            self.tables[tablename] = { "updates" : 1, 
                                       "nr_rows" : rowcount,
                                       "last_update_dt" : now }
        else:
            if not update:
                self.tables[tablename]["nr_rows"] += rowcount

            self.tables[tablename]["updates"] += 1
            self.tables[tablename]["last_update_dt"] = now

        # Keep track of which "identities" have added to a table.

        if identity not in self.connections:

            self.connections[identity] = { 'tables' : { tablename : rowcount }, 
                                           'last_update_dt' : now }
        else:
            self.connections[identity]["last_update_dt"] = now

            if tablename not in self.connections[identity]["tables"]:
                self.connections[identity]["tables"][tablename] = rowcount
            elif not update:
                self.connections[identity]["tables"][tablename] += rowcount
            
        return 1



