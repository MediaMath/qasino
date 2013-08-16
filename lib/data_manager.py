
import sqlite_backend as sql_backend

import table_merger

import logging
import time
import re

from twisted.internet import threads

class DataManager(object):

    def __init__(self, use_dbfile, db_dir=None, signal_channel=None, archive_db_dir=None, generation_duration_s=None):

        self.saved_tables = {}
        self.query_id = 0

        self.generation_duration_s = generation_duration_s
        self.signal_channel = signal_channel
        self.archive_db_dir = archive_db_dir

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
        self.sql_backend_writer = sql_backend.SqlConnections(db_file_name, self, self.archive_db_dir)

        # Call swap dbs to make the writer we just opened the reader
        # and to open a new writer.

        self.rotate_dbs()


    def get_query_id(self):
        self.query_id += 1
        return self.query_id

    def shutdown(self):
        if self.sql_backend_reader:
            self.sql_backend_reader = None

        if self.sql_backend_writer:
            self.sql_backend_writer = None


    def async_validate_and_route_query(self, sql, query_id, use_write_db=False):
        if use_write_db:
            return self.sql_backend_writer.dbpool.runInteraction(self.validate_and_route_query, sql, query_id, self.sql_backend_writer)
        else:
            return self.sql_backend_reader.dbpool.runInteraction(self.validate_and_route_query, sql, query_id, self.sql_backend_reader)


    def validate_and_route_query(self, txn, sql, query_id, sql_backend):

        m = re.search(r"^\s*select\s+", sql, flags=re.IGNORECASE)
        if m == None:

            # Process a non-select statement.

            return self.process_non_select(txn, sql, query_id, sql_backend)

        # Proecess a select statement.

        return sql_backend.do_select(txn, sql)


    def process_non_select(self, txn, sql, query_id, sql_backend):
        """
        Called for non-select statements like show tables and desc.
        """

        # desc?

        m = re.search(r"^\s*desc\s+(\S+)\s*;$", sql, flags=re.IGNORECASE)

        if m != None:
            (retval, error_message, table) = sql_backend.do_desc(txn, m.group(1))

            result = { "retval" : retval }
            if error_message:
                result["error_message"] = error_message
            if table:
                result["data"] = table

            return result

        # show tables?

        m = re.search(r"^\s*show\s+tables\s*;$", sql, flags=re.IGNORECASE)
        if m != None:
            return sql_backend.do_select(txn, "SELECT * FROM qasino_table_info;")


        # Exit?

        m = re.search(r"^\s*(quit|logout|exit)\s*;$", sql, flags=re.IGNORECASE)
        if m != None:
            return { "retval" : 0, "error_message" : "Bye!" }


        return { "retval" : 1, "error_message" : "ERROR: Unrecognized statement: %s" % sql }


    def get_table_list(self):

        return self.sql_backend_reader.tables


    # This ugly hack insures all the internal tables are inserted
    # using the same sql_backend_writer and makes sure that the
    # "tables" table is called last (after all the other internal
    # tables are added).

    def insert_connections_table_complete(self, result, sql_backend_writer):
        # Finally we can insert the tables table.
        sql_backend_writer.async_insert_tables_table()

    def insert_status_table_complete(self, result, sql_backend_writer):
        # Next insert the connections table.
        d = sql_backend_writer.async_insert_connections_table()
        d.addCallback(self.insert_connections_table_complete, sql_backend_writer)

    def insert_internal_tables(self):
        # Start with the status table.

        d = self.sql_backend_writer.async_insert_status_table(self.db_generation_number)

        # We must pass the sql_backend_writer through the callback
        # chain and use it rather then self.sql_backend_writer because
        # self.sql_backend_writer might get "swapped" between async calls.

        d.addCallback(self.insert_status_table_complete, self.sql_backend_writer)


    def rotate_dbs(self):
        """ 
        Make the db being written to be the reader db.
        Open a new writer db for all new updates.
        """

        logging.info("**** DataManager: Starting generation %d", self.db_generation_number)

        # Before making the write db the read db, 
        # add various internal info tables.

        self.insert_internal_tables()

        # Increment the generation number.

        self.db_generation_number = int(time.time())

        # Set the writer to a new db

        save_sql_backend_writer = self.sql_backend_writer

        # If specified put the generation number in the db name.

        db_file_name = self.db_name

        if not self.one_db:
                db_file_name = self.db_name % self.db_generation_number

        self.sql_backend_writer = sql_backend.SqlConnections(db_file_name, self, self.archive_db_dir)

        # Set the reader to what was the writer

        self.sql_backend_reader = save_sql_backend_writer

        # Load saved tables.

        self.async_add_saved_tables()

        # Note the reader will (should) be deconstructed here.

        # Lastly blast out the generation number.

        if self.signal_channel != None:
            self.signal_channel.send_generation_signal(self.db_generation_number, self.generation_duration_s)

    def check_save_table(self, tablename, table, identity, persist, update):

        if persist:
            self.saved_tables[tablename] = { "table" : table, "identity" : identity, "update" : True }

        else:
            # Be sure to remove a table that is no longer persisting.
            if tablename in self.saved_tables:
                del self.saved_tables[tablename]


    def async_add_saved_tables(self):

        for tablename, table_data in self.saved_tables.iteritems():

            logging.info("Adding saved table '%s' from '%s'", tablename, table_data["identity"])

            update = True if "update" in table_data and table_data["update"] else False

            self.sql_backend_writer.async_add_table_data(table_data["table"], table_data["identity"], persist=True, update=update)
