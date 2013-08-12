#!/usr/bin/python

import sys
import os
import logging
from pprint import pprint
from optparse import OptionParser
import simplejson
import re

from twisted.internet import reactor
from twisted.internet import task


for path in [
    os.path.join('opt', 'qasino', 'lib'),
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib'))
]:
    if os.path.exists(os.path.join(path, '__init__.py')):
        sys.path.insert(0, path)
        break

from txzmq import ZmqFactory

from csv_table_reader import CsvTableReader
import json_requestor
import json_subscriber
import constants
from util import Identity


def get_csv_files_from_index(index_file):
    """
    Read the csv filenames from an index file.
    Returns a list of pairs of filename and tablename.
    """

    results = []

    fh = open(index_file, 'r')

    for lineno, line in enumerate(fh):

        # Skip the first line, its a useless version number.

        if lineno == 0:
            continue

        # Remove newlines at the end.
        line = line.rstrip('\r\n')

        # If the line contains a comma (,) then it has filename and tablename.

        fields = line.split(',')

        if len(fields) > 1:
            filename = fields[0]
            tablename = fields[1]

        else:
            tablename = fields[0]
            filename = fields[0] + ".csv"
            
        results.append( [ filename, tablename ] )

    fh.close()

    return results


def get_index_list_file_indexes(index_list_file):
    """
    Reads an "index list file" to get a list if indexes to process.
    Returns a list of index file paths.
    """

    result_indexes = []

    # Read the file - each line is the full path to an index file.

    fh = open(index_list_file, "r")

    for line in fh:

        # Skip comments
        m = re.match(r'\s*#', line)
        if m: continue
        
        # Skip blank lines
        m = re.match(r'\s*$', line)
        if m: continue

        # Remove newlines at the end.
        line = line.rstrip("\r\n")

        result_indexes.append(line)

    fh.close()

    return result_indexes


def get_table_list_file_tables(table_list_file):
    """
    Reads a "table list file" to get a list if tables to limit to.
    Returns a list of table names.
    """

    result_tables = []

    # Read the file - each line is the tablename

    fh = open(table_list_file, "r")

    for line in fh:

        # Skip comments
        m = re.match(r'\s*#', line)
        if m: continue
        
        # Skip blank lines
        m = re.match(r'\s*$', line)
        if m: continue

        # Remove newlines at the end.
        line = line.rstrip("\r\n")

        result_tables.append(line)

    fh.close()

    return result_tables


def read_and_send_tables(json_requestor, options):
    """
    Given the specified indexes, read the csv files and publish them
    to a qasino server.
    """

    # Make a table whitelist lookup dict.
    table_whitelist = {}
    use_table_whitelist = False

    # Was there one more tables given on the command line?

    if options.tables and len(options.tables) > 0:
        use_table_whitelist = True
        for x in options.tables:
            table_whitelist[x] = 1

    # Was there a table list file given on the command line?

    if options.table_list != None and len(options.table_list) > 0:

        tables_from_file = get_table_list_file_tables(options.table_list)

        if tables_from_file and len(tables_from_file) > 0:
            use_table_whitelist = True
            for x in tables_from_file:
                table_whitelist[x] = 1


    # Create a csv table reader object.

    csv_table_reader = CsvTableReader()

    # This will be the list of indexes to process.
    indexes = []

    # Was there one or more index files given on the command line?

    if options.indexes: 
        indexes = indexes + options.indexes

    # Was there an index list file given on the command line?

    if options.index_list:

        indexes_from_file = get_index_list_file_indexes(options.index_list)

        if indexes_from_file:
            indexes = indexes + indexes_from_file


    # Now process all the indexes.

    for index_file in indexes:

        index_dir = os.path.dirname(index_file)

        csv_files = get_csv_files_from_index(index_file)

        if csv_files == None or len(csv_files) <= 0:

            logging.info("Warning: no csv files found in index '%s'", index_file)
            continue

        for csv_file_item in csv_files:

            (filename, tablename) = csv_file_item

            # Is this a tablename we can process?

            if use_table_whitelist:
                if tablename not in table_whitelist:
                    continue

            filepath = '/'.join( [index_dir, filename] )

            logging.info("Reading file '%s'.", filepath)

            # The csv files we'll be reading in will have this format:
            # ------------------------
            # version
            # tablename
            # column descriptions (csv)
            # column types (csv)
            # column names (csv)
            # data (csv)
            # ...
            # ------------------------

            # Ignore the first three lines.  Types in 4th, names in 5th.
            table = csv_table_reader.read_table(filepath, tablename,
                                                skip_linenos={0, 1, 2},
                                                types_lineno=3,
                                                colnames_lineno=4)

            if table == None:
                logging.info("Failure reading csv file '%s'.", filepath)
                continue

            logging.info("Sending table '%s' to '%s:%d' (%d rows).", tablename, options.hostname, options.port, len(table["rows"]))

            json_requestor.send_table(table)




if __name__ == '__main__':

    logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.INFO)

    parser = OptionParser()

    parser.add_option("-I", "--identity", dest="identity",
                      help="Use IDENTITY as identity", metavar="IDENTITY")

    parser.add_option("-H", "--hostname", dest="hostname", default='localhost',
                      help="Send table to HOSTNAME qasino server", metavar="HOSTNAME")

    parser.add_option("-p", "--port", dest="port", default=constants.JSON_RPC_PORT,
                      help="Use PORT for qasino server", metavar="PORT")

    parser.add_option("-P", "--pubsub-port", dest="pubsub_port", default=constants.JSON_PUBSUB_PORT,
                      help="Use PORT for qasino pubsub connection", metavar="PORT")

    parser.add_option("-i", "--index", dest="indexes",
                      action="append",
                      help="Path to a index file to process" )

    parser.add_option("-f", "--index-list", dest="index_list",
                      help="Path to a file with a list of index files to process in it" )

    parser.add_option("-T", "--table", dest="tables",
                      action="append",
                      help="Tables to limit publishing to" )

    parser.add_option("-t", "--table-list", dest="table_list",
                      help="Path to a file with a list of tables to limit publishing to" )


    (options, args) = parser.parse_args()

    if options.identity != None:
        Identity.set_identity(options.identity)

    if options.hostname == None:
        logging.info("Please specify a hostname to connect to.")
        exit(1)

    zmq_factory = ZmqFactory()

    # Create a json request object.

    logging.info("Connecting to %s:%d to send tables.", options.hostname, options.port)

    json_requestor = json_requestor.JsonRequestor(options.hostname, options.port, zmq_factory)

    # Create a json subscriber object.

    logging.info("Connecting to %s:%d to listen for generation signals.", options.hostname, options.pubsub_port)

    json_subscriber = json_subscriber.JsonSubscriber(options.hostname, options.pubsub_port, zmq_factory)


    # Read and send the table when a generation signal comes in.

    json_subscriber.subscribe_generation_signal(read_and_send_tables, json_requestor, options)

    # Read and send the table at a fixed interval.

#    request_metadata_task = task.LoopingCall(read_and_send_table, json_requestor, options)
#    request_metadata_task.start(options.interval)

    # Read and send immediately, uncomment.

#    read_and_send_tables(json_requestor, options)

    # Run the event loop

    reactor.run()
