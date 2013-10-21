Qasino
======

Qasino is a stats collection system that supports tabular stats
stored in a database for querying with SQL.  Stats are ephemeral,
meaning only the latest stats reported for a given table are available
in the database.  The same table reported from multiple machines is
merged automatically.

Currently qasino is implemented in python using the twisted
framework and http and zeromq transports with a sqlite backend data store.

##Installation

You'll need to have the following python libraries installed:
- python-twisted
- python-zmq
- python-simplejson
- python-httplib2
- python-apsw
- python-yaml

##Running

The server and the client can be run right from the main directory.
They will find the libraries they need in the lib directory. To run the server:

    python bin/qasino_server.py

Connect with the SQL client:

    python bin/qasino_sqlclient.py -Hqasino-server-hostname

To run the CSV publisher:

    python bin/qasino_csvpublisher.py --identity 1.2.3.4 --index-file index.csv

##Overview

The qasino server receives requests from clients to publish tabular data.  
The data is added to a backend sqlite database.  The data is collected
for a fixed period (default 30 seconds) after which it is snapshotted.
The snapshotted database becomes the data source for all incoming SQL
requests until the next snapshot.  For this reason all publishers
need to publish updated stats every snapshot period.

To orchestrate this process better the server publishes on a ZeroMQ
pub-sub channel the snapshot (aka generation) signal.  This should
trigger all publishers to send table data.  qasino_cvspublisher
works this way.  It is by no means required.  In fact a simpler
approach is just to have all publishers publish their data on 
an interval that matches the generation interval.

##Querying (SQL)

Qasino can be queried with SQL using three different methods:

###Line receiver

Connect to a qasino server on port 15000 for line based text only queries.  You can 
simply connect using telnet and send your query.

    $ telnet 1.2.3.4 15000
    Trying 1.2.3.4...
    Connected to 1.2.3.4.
    Escape character is '^]'.
    select * from qasino_server_info;
    generation_number  generation_duration_s  generation_start_epoch
    =================  =====================  ======================
           1382105093                     30            1382105123.1
    1 rows returned

###Python Client

Connect using bin/qasino_sqlclient.py.  This client uses ZeroMQ to 
send JSON formated messages to the server.

    $ bin/qasino_sqlclient.py -H1.2.3.4
    Connecting to 1.2.3.4:15598.
    qasino> select * from qasino_server_info;
    generation_number  generation_duration_s  generation_start_epoch
    =================  =====================  ======================
           1382119193                     30            1382119223.1
    1 rows returned
    qasino> 

It uses a json message with the following simple format:

    {
        "op" : "query",
        "sql" : "select * from qasino_server_info;" 
    }


###HTTP Interface

Lastly you can connect with a simple HTTP request.  There are a couple variations.

First you can POST a JSON request:

    $ curl -X POST 'http://1.2.3.4:15597/request?op=query' -d '{ "sql" : "select * from qasino_server_info;" }'
    {"table": {"rows": [["1382119553", "30", "1382119583.1"]], "column_names": ["generation_number", "generation_duration_s", "generation_start_epoch"]}, "max_widths": {"1": 21, "0": 17, "2": 22}, "response_op": "result_table", "identity": "1.2.3.4"}
    $

Or you can make a GET request with 'sql' as a query string param (be sure to url-encode it):

    $ curl 'http://localhost:15597/request?op=query' --get --data-urlencode 'sql=select * from qasino_server_info;'
    {"table": {"rows": [["1382131545", "30", "1382131575.89"]], "column_names": ["generation_number", "generation_duration_s", "generation_start_epoch"]}, "max_widths": {"1": 21, "0": 17, "2": 22}, "response_op": "result_table", "identity": "1.2.3.4"}

Or make a GET request with the 'format=text' query string parameter to get a human readable rendering of the table.

    $ curl 'http://1.2.3.4:15597/request?op=query&format=text' --get --data-urlencode 'sql=select * from qasino_server_info;'
    generation_number  generation_duration_s  generation_start_epoch
    =================  =====================  ======================
           1382131635                     30           1382131665.89
    1 rows returned
    $

##Publishing

Currently the only client officially implemented is a CSV file publisher.  
The CSV publisher takes an index file with a list of CSV files in it to
publish and/or a index list file with a list of index files to
process in the same manner.

###Index File Format

An index file starts with a version number followed by one or more CSV tables to publish.

Each line specifying a table to publish contains either a filename and tablename or just a tablename where the filename is inferred.

So either:

    <filename>,<tablename>

or just

    <tablename>

In the latter case the filename is inferred to be `<tablename>.csv`.

So for example you might have an index file `myindex.csv` like the following:

    1
    myapplication_table1
    myapplication_table2

###CSV File Format

The CSV files contain the following format:

    <version number>
    <tablename>
    <column names>
    <column types>
    <column descriptions>
    <data row1>
    <data row2>
    ...

So for example you might create a file `myapplication_table1.csv`:

    1
    myapplication_table1
    ipaddr,datacenter,stat1,stat2
    ip,string,integer,integer
    IP Address,Datacenter,Num Frobs,Num Foobs
    1.2.3.4,BOS,123,456

And then you might run the csv publisher like this:

    python bin/qasino_csvpublisher.py --index myindex.csv --identity 1.2.3.4

###HTTP Publishing

You can publish by sending a properly formatted JSON request via an HTTP connection.

For example to publish the same "myapplication_table1" table you could put the following in a file `myapplication_table1.json`:

    { "op" : "add_table_data",
      "identity" : "1.2.3.4",
      "table" : {  "tablename" : "myapplication_table1",
                   "column_names" : [ "ipaddr", "datacenter", "stat1", "stat2" ],
                   "column_types" : [ "ip", "string", "int", "int" ],
                   "rows" : [ [ "1.2.3.4", "BOS", 123, 456 ] ]
                }
    }

And then send the following curl:

    $ curl -d @myapplication_table1.json -X POST 'http://1.2.3.4:15597/request?op=add_table_data'
    {"identity": "1.2.3.4", "response_op": "ok"}
    $

The table should appear in Qasino.  Publishing would have to happen regularly for it to persist.

    $ bin/qasino_sqlclient.py
    Connecting to 1.2.3.4:15598.
    qasino> select * from qasino_server_tables where tablename = 'myapplication_table1';
               tablename
    ====================
    myapplication_table1
    1 rows returned
    qasino> select * from myapplication_table1;
     ipaddr  datacenter  stat1  stat2
    =======  ==========  =====  =====
    1.2.3.4         BOS    123    456
    1 rows returned
    qasino>
