#!/usr/bin/python

import sys
import os
import logging
from pprint import pprint
from optparse import OptionParser
import re
import random
import time

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
import constants
from util import Identity
import qasino_table


def main():

    global options

    logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.INFO)

    parser = OptionParser()

    parser.add_option("-I", "--identity", dest="identity",
                      help="Use IDENTITY as identity", metavar="IDENTITY")

    parser.add_option("-H", "--hostname", dest="hostname", default='localhost',
                      help="Send table to HOSTNAME qasino server", metavar="HOSTNAME")

    parser.add_option("-p", "--port", dest="port", default=constants.ZMQ_RPC_PORT,
                      help="Use PORT for qasino server", metavar="PORT")

    parser.add_option("-u", "--username", dest="username", 
                      help="HTTPS auth username")

    parser.add_option("-w", "--password", dest="password", 
                      help="HTTPS auth password")

    parser.add_option("-P", "--pubsub-port", dest="pubsub_port", default=constants.ZMQ_PUBSUB_PORT,
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

    parser.add_option("-d", "--send-delay-max", dest="send_delay_max", default=15,
                      help="Max delay to add when its time to send tables." )

    parser.add_option("-x", "--interval", dest="interval", default=None,
                      help="Interval to send updates (This will turn off subscribing)." )

    parser.add_option("-s", "--use-https", dest="use_https", default=False, action="store_true",
                      help="Use HTTP over SSL/TLS protocol to publish table.")

    parser.add_option("-k", "--skip-ssl-verify", dest="skip_ssl_verify", default=False, action="store_true",
                      help="Don't verify SSL certificates.")

    parser.add_option("-g", "--gen-signal-timeout", dest="gen_signal_timeout", default=120,
                      help="Timeout after which we restart the generation signal subscription.")

    (options, args) = parser.parse_args()

    logging.info("Qasino csv publisher starting")

    if options.identity != None:
        Identity.set_identity(options.identity)

    logging.info("Identity is %s", Identity.get_identity())

    if options.hostname == None:
        logging.info("Please specify a hostname to connect to.")
        exit(1)

    zmq_factory = ZmqFactory()

    # Create a request object (either ZMQ or HTTPS).

    if options.use_https:
        import http_requestor

        # Change the default port if we're https
        if options.port == constants.ZMQ_RPC_PORT:
            options.port = constants.HTTPS_PORT

        logging.info("Connecting to {}:{} with HTTPS to send tables.".format(options.hostname, options.port))

        # Disable extraneous logging in requests.
        requests_log = logging.getLogger("requests")
        requests_log.setLevel(logging.WARNING)

        requestor = http_requestor.HttpRequestor(options.hostname, options.port, 
                                                 username = options.username,
                                                 password = options.password, 
                                                 skip_ssl_verify = options.skip_ssl_verify )
    else:  # Use zmq requestor
        import zmq_requestor

        logging.info("Connecting to {}:{} with ZeroMQ to send tables.".format(options.hostname, options.port))

        requestor = zmq_requestor.ZmqRequestor(options.hostname, options.port, zmq_factory)

    # Determine the update trigger (interval or signal).

    if options.interval is None or options.interval < 10:

        logging.info("Connecting to {}:{} on pubsub ZeroMQ channel to listen for generation signals.".format(options.hostname, options.pubsub_port))

        # Create a zeromq pub sub subscriber.

        import zmq_subscriber

        zmq_subscriber = zmq_subscriber.ZmqSubscriber(options.hostname, options.pubsub_port, zmq_factory)

        # Read and send the table when a generation signal comes in.

        global last_gen_signal_time

        last_gen_signal_time = time.time()

        zmq_subscriber.subscribe_generation_signal(initiate_read_and_send_tables, requestor, options)

        # Set a timeout so we can restart the subscribe if we haven't heard from the server in a while.

        reactor.callLater(5, check_for_gen_signal_timeout, options, zmq_subscriber, requestor, zmq_factory)

    else:
        # Read and send the table at a fixed interval.

        logging.info("Sending data on fixed interval ({} seconds).".format(options.interval))

        request_metadata_task = task.LoopingCall(read_and_send_tables, requestor, options)
        request_metadata_task.start(options.interval)


    # Read and send immediately, uncomment.

#    read_and_send_tables(requestor, options)

    # Run the event loop

    reactor.run()

    logging.info("Qasino csv publisher exiting")


def check_for_gen_signal_timeout(options, prev_zmq_subscriber, requestor, zmq_factory):

    global last_gen_signal_time

    if time.time() - last_gen_signal_time > options.gen_signal_timeout:
        logging.warning("Exceeded the generation signal timeout (%ds).  Restarting ZeroMQ subscriber.", options.gen_signal_timeout)
        
        import zmq_subscriber

        prev_zmq_subscriber.shutdown()

        zmq_subscriber = zmq_subscriber.ZmqSubscriber(options.hostname, options.pubsub_port, zmq_factory)

        # Read and send the table when a generation signal comes in.

        zmq_subscriber.subscribe_generation_signal(initiate_read_and_send_tables, requestor, options)

        # Reset the timer and last signal time so we don't re-subscribe right away.
        last_gen_signal_time = time.time()
        reactor.callLater(5, check_for_gen_signal_timeout, options, zmq_subscriber, requestor, zmq_factory)
    else:

        # Reset the timer to check again in a bit.
        reactor.callLater(5, check_for_gen_signal_timeout, options, prev_zmq_subscriber, requestor, zmq_factory)
        


def get_csv_files_from_index(index_file):
    """
    Read the csv filenames from an index file.
    Returns a list of pairs of filename and tablename.
    """

    results = []

    try:
        fh = open(index_file, 'r')
    except Exception as e:
        logging.info("Failed to open index file '%s': %s", index_file, e)
        return

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

    try:
        fh = open(index_list_file, "r")
    except Exception as e:
        logging.info("Failed to open index list file '%s': %s", index_list_file, e)
        return

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

    try:
        fh = open(table_list_file, "r")
    except Exception as e:
        logging.info("Failed to open table list file '%s': %s", table_list_file, e)
        return

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


def initiate_read_and_send_tables(requestor, options):
    """ 
    Calls read_and_send_tables after a random delay to reduce "storming" the server.
    """
    global last_gen_signal_time

    last_gen_signal_time = time.time()

    delay = random.randint(0, int(options.send_delay_max))
    logging.info("Waiting %d seconds to send data.", delay)
    reactor.callLater(delay, read_and_send_tables, requestor, options)


def read_and_send_tables(requestor, options):
    """
    Given the specified indexes, read the csv files and publish them
    to a qasino server.
    """

    nr_tables = 0
    nr_errors = 0
    table_info = {}

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
            # version/options
            # tablename
            # column names (csv)
            # column types (csv)
            # column descriptions (csv)
            # data (csv)
            # ...
            # ------------------------

            table_info[tablename] = {}
            table_info[tablename]["filepath"] = filepath
            table_info[tablename]["nr_rows"] = -1
            table_info[tablename]["nr_errors"] = 0
            table_info[tablename]["error_msg"] = ''
            table_info[tablename]["read_epoch"] = time.time()

            try:
                filehandle = open(filepath, 'r')
            except Exception as e:
                nr_errors += 1
                table_info[tablename]["nr_errors"] = 1
                table_info[tablename]["error_msg"] = e.str()
                logging.info("Failure reading csv file '%s': %s", filepath, error)
                continue

            # Ignore the 2nd and 5th lines.  Names in 3rd, types in 4th.  The "version" in the first line is now the options list.
            (table, error) = csv_table_reader.read_table(filehandle, tablename,
                                                         skip_linenos={1, 4},
                                                         options_lineno=0,
                                                         types_lineno=3,
                                                         colnames_lineno=2)

            filehandle.close()

            table_info[tablename]["read_time_s"] = time.time() - table_info[tablename]["read_epoch"]

            if table == None:
                nr_errors += 1
                table_info[tablename]["nr_errors"] = 1
                table_info[tablename]["error_msg"] = error
                logging.info("Failure reading csv file '%s': %s", filepath, error)
                continue

            nr_tables += 1

            table_info[tablename]["nr_rows"] = table.get_nr_rows()

            properties = []
            if table.get_property('static'): properties.append(' static')
            if table.get_property('update'): properties.append(' update')
            if table.get_property('persist'): properties.append(' persist')

            logging.info("Sending{} table '{}' to '{}:{}' ({} rows).".format( ''.join(properties), tablename, options.hostname, options.port, table_info[tablename]["nr_rows"] ) )

            error = requestor.send_table(table)

            if error is not None:
                logging.info("Error sending table '{}': {}".format(tablename, error))
        
        # END for each csv file

    # END for each index

    # Publish an info table

    publish_info_table(requestor, nr_tables, nr_errors)

    # Publish a table list table.

    publish_tables_table(requestor, table_info)


def publish_info_table(requestor, nr_tables, nr_errors):

    tablename = "qasino_csvpublisher_info"

    table = qasino_table.QasinoTable(tablename)
    table.add_column("identity", "varchar")
    table.add_column("update_epoch", "int")
    table.add_column("nr_tables", "int")
    table.add_column("nr_errors", "int")
    table.add_row( [ Identity.get_identity(), time.time(), nr_tables, nr_errors ] )

    logging.info("Sending table '%s' to '%s:%d' (1 rows).", tablename, options.hostname, options.port)

    requestor.send_table(table)

def publish_tables_table(requestor, table_info):

    this_tablename = "qasino_csvpublisher_tables"

    table = qasino_table.QasinoTable(this_tablename)
    table.add_column("identity", "varchar")
    table.add_column("tablename", "varchar")
    table.add_column("read_epoch", "int")
    table.add_column("read_time_s", "int")
    table.add_column("nr_errors", "int")
    table.add_column("error_msg", "int")
    table.add_column("nr_rows", "int")
    table.add_column("filepath", "varchar")

    for tablename, table_stats in table_info.iteritems():
        table.add_row( [ Identity.get_identity(), 
                         tablename,
                         table_stats["read_epoch"], 
                         table_stats["read_time_s"], 
                         table_stats["nr_errors"],
                         table_stats["error_msg"],
                         table_stats["nr_rows"],
                         table_stats["filepath"] ] )
        
    logging.info("Sending table '%s' to '%s:%d' (%d rows).", this_tablename, options.hostname, options.port, table.get_nr_rows())

    requestor.send_table(table)



if __name__ == '__main__':

    exit( main() )
