Qasino
======

Qasino is a stats server that supports stats stored as tables in a
database for querying with SQL.  Unlike a conventional database, stats
do not accumulate over time (which would make querying the stats more
difficult).  Instead Qasino keeps only the most recent set of stats
reported.  The full flexiblity of SQL including joining, filtering and
aggregation are availble to analyze the current state of your network.
Stats reported from different systems but using the same table name
are merged automatically into a single table.  In addition schemas are
automatically created and updated based on the incoming updates
requiring zero maintanence.

Many stats systems provide history for stats but lack an effective way
to join, correlate or connect stats together.  Qasino does not
(directly) keep a history of stats but does make it really easy to
correlate and cross reference stats together.

Sometimes you want to know the current state of your systems and that
might include more then just numerical data.  Qasino excels at giving
you that information.  You can report a richer set of data such as
text data, datetimes or multi-row relationships.  For example, cpu
usage and ops per second, or configuration files with the md5sum of
each and current timestamp.

Qasino primarily is a server process.  It exposes interfaces to
publish tables to it using a JSON API via HTTP or ZeroMQ.  There is a
simple CLI client to connect to Qasino and run queries.  There is a
simple command line table publisher that can read input tables as CSV
or JSON.  And there is a more advanced publisher client meant to run
as an agent that dynamically publishes CSV files.

In the future there will be integration with stat collectors like
Statsd, Diamond and Graphite.

Currently Qasino is implemented in Python using the Twisted framework
and HTTP and ZeroMQ transports with Sqlite for the backend data store.

Qasino was inspired by the monitoring system used at Akamai
Technologies called Query.  More information can be found 
[here](http://www.akamai.com/dl/technical_publications/lisa_2010.pdf "Keeping Track of 70,000+ Servers: The Akamai Query System") 
and [here](http://www.akamai.com/dl/technical_publications/query_osr.pdf "Scaling a Monitoring Infrastructure for the Akamai Network").
Qasino provides similar functionality but for much smaller scale environments.

##Installation

You'll need to have the following python libraries installed:

- python-twisted
- python-zmq
- python-apsw
- python-yaml
- python-requests
- python-txzmq
- python-jinja2

Alternately, you can build qasino using [Docker](https://www.docker.com/).
This will let you deploy qasino in a Docker container.  Simply call the Docker
build command on the Dockerfile included in this repo:

    docker build -t="my-container-name" /path/to/qasino/Dockerfile

##Running

The server and the client can be run right from the main directory.
They will find the libraries they need in the lib directory. To run the server:

    python bin/qasino_server.py

Connect with the SQL client:

    python bin/qasino_sqlclient.py -Hqasino-server-hostname

To run the CSV publisher:

    python bin/qasino_csvpublisher.py --identity 1.2.3.4 --index-file index.csv
    
To run using Docker, call `docker run` on the container you built.  You need
to use the `-P` flag (or set port mappings manually) in order to send requests
to the qasino server.  For example:

    docker run -P my-container-name /opt/qasino/bin/qasino_server.py

You can find the ports that Docker assigned to your qasino container using
`docker ps`.

##Overview

The qasino server receives requests from clients to publish tabular
data.  The data is added to a backend sqlite database.  The data is
collected for a fixed period (default 30 seconds) after which it is
snapshotted.  The snapshotted database becomes the data source for all
incoming SQL requests until the next snapshot.  For this reason all
publishers need to publish updated stats every snapshot period (with
the exception of static tables or persistent tables which are
described below).

To orchestrate this process better the server publishes on a ZeroMQ
pub-sub channel the snapshot (aka generation) signal.  This should
trigger all publishers to send table data.  qasino_cvspublisher works
this way.  It is by no means required though.  In fact a simpler
approach is just to have all publishers publish their data on an
interval that matches the generation interval.

##Querying (SQL)

Qasino has a SQL interface.  Since Qasino uses SQLite on the backend
you can refer to the [SQLite SQL documentation](http://www.sqlite.org/lang.html) 
for SQL syntax details.  Qasino can be queried with SQL using four different methods:

###Web UI

Point your browser at Qasino and you'll get a basic web interface.
The default page shows all the tables that Qasino knows about.  There
are also tabs for describing tables and inputing custom SQL
statements.

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
send JSON formated messages to the server.  (It can also connect using
HTTPS given the --use-https option but that will require the right
credentials to work).

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

Lastly you can connect with a simple HTTP request.  The default HTTP
port is 15597.  These requests can also go to the SSL port 443 but
will require basic auth.  There are a couple variations.

First you can POST a JSON request:

    $ curl -X POST 'http://1.2.3.4:15597/request?op=query' -d '{ "sql" : "select * from qasino_server_info;" }'
    {"table": {"rows": [["1382119553", "30", "1382119583.1"]], "column_names": ["generation_number", "generation_duration_s", "generation_start_epoch"]}, "max_widths": {"1": 21, "0": 17, "2": 22}, "response_op": "result_table", "identity": "1.2.3.4"}
    $

Or you can make a GET request with 'sql' as a query string param (be sure to url-encode it):

    $ curl 'http://localhost:15597/request?op=query' --get --data-urlencode 'sql=select * from qasino_server_info;'
    {"table": {"rows": [["1382131545", "30", "1382131575.89"]], "column_names": ["generation_number", "generation_duration_s", "generation_start_epoch"]}, "max_widths": {"1": 21, "0": 17, "2": 22}, "response_op": "result_table", "identity": "1.2.3.4"}

Or make a GET request with the 'format=text' query string parameter to get a human readable rendering of the table.  Note you can also put this url right in a browser.

    $ curl 'http://1.2.3.4:15597/request?op=query&format=text' --get --data-urlencode 'sql=select * from qasino_server_info;'
    generation_number  generation_duration_s  generation_start_epoch
    =================  =====================  ======================
           1382131635                     30           1382131665.89
    1 rows returned
    $

###Internal Tables

Qasino server automatically publishes the following internal tables:
- qasino_server_info
- qasino_server_tables
- qasino_server_connections
- qasino_server_views

The following commands are shortcuts to looking at these tables:
- SHOW info;
- SHOW tables;
- SHOW connections;
- SHOW views;

The schema for a table can be found with 'DESC <tablename>;' and the
definition of a view can be found with 'DESC VIEW <viewname>;'


##Publishing

Currently the only publishing clients officially implemented are a CSV
file publisher that is meant to run as an agent remotely and publishes
CSV files as tables (qasino_csvpublisher.py) and a command line
utility to publish one-offs from JSON or CSV input files (qasino_publish.py).

All publishers must specify an "identity" which is a unique identifier
that tells the server where the input table is coming from.  A given
tablename can only be reported from the same identity once per
reporting cycle but different identities can report rows for the same
tablename - the results will be merged together.  

A common paradigm is to make the identity the hostname or IP address
of the machine reporting the rows.  In addition it is suggested to
include a column in the table that indicates the same thing (identity,
hostname or IP address).  In this way, two nifty things happen:

1. All machines reporting rows for a common table (lets say 'table_foo') will be merged together.
2. You can make queries that select by machine e.g. "SELECT * FROM table_foo WHERE identity IN ('1.2.3.4', '5.6.7.8');"

###Schema Merging

Typically all your publishers have the same schema for a given table
but if its different (perhaps if you are rolling out a new release
that adds columns to a table) the server will always add columns that
don't already exist in the table.  The schema you will end up with
will be a union of all the columns.  

Changing types of an existing column is not recommended (there may be
some undefined behavior).  Just add a new column with the different
type.

###Types

CSV input tables (see below) support the following types (and are converted into the types in the JSON list below):
- string (also str)
- ip (alias for string)
- float
- integer (also int)
- ll (alias for integer)
- time (alias for integer)

JSON input tables (see below) support the following types (which are sqlite types):
- integer
- real 
- text

###Qasino_csvpublisher.py

qasino_csvpublisher.py takes an index file with a list of CSV files in
it to publish and/or a index list file with a list of index files to
process in the same way.  It runs until killed and monitors the
indexes and tables they refer to for changes.  The data is continually
published to the server every cycle so that the CSV content is always
reflected in tables on the server.  The intent is so that applications
or processes can simply drop properly formated CSV files into
locations and they will automatically get loaded and published to the
server.

####Index File Format

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

####CSV File Format

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

###Qasino_publish.py

The command line utility qasino_publish.py reads from file or stdin an
input table (CSV or JSON) and sends it via HTTP, HTTPS or ZeroMQ to a
server.  Its largely meant as an example client but could be used in a
cron or script.  See --help for more information.  See above for CSV file format.

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

###Persistent tables

A node can publish a table with the option 'persist' to indicate that
the table should be carried through each generation.  An option is
given in the top level dict of the JSON object.  For example:

    { "op" : "add_table_data",
      "persist" : 1,
      "identity" : "1.2.3.4",
      "table" : {  "tablename" : "myapplication_table1",
                   "column_names" : [ "ipaddr", "datacenter", "stat1", "stat2" ],
                   "column_types" : [ "ip", "string", "int", "int" ],
                   "rows" : [ [ "1.2.3.4", "BOS", 123, 456 ] ]
                }
    }

Some things to note.

- The stats are carried forward for each successive generation so that means the server has to hold onto an extra copy of the stats which can consume additional memory.
- If the server is restarted the persistent stats will go away until they are resent.
- The tables are tracked per tablename (at the moment), so multiple updates for the same table overwrite.

###Static tables

Static tables are set using the "static" option similar to persist
above but get loaded into a special persistent Sqlite DB that is
connected to the ephemeral databases.  Tables that are static persist
between Qasino server restarts.  They are also stored by tablename so
multiple updates to the same table overwrite.

###Views

Views are supported by using a view configuration file.  The Qasino
server will look by default (change it with the --views-file option)
for a file in the current directory called 'views.conf' that is a YAML
file that has the following format:

    ---
    
    - viewname: testview
      view: | 
        create view testview as select 
          * from qasino_server_info;

    - viewname: anotherview
      view: | 
        create view anotherview as select 
          * from qasino_server_tables;

It is an array of items with 'viewname' and 'view' properties.  The
'view' property specifies the actual view to create.  

The views file is monitored for changes and automatically reloaded.

You can get the definition of a view from the qasino_server_views
table or the 'DESC VIEW' command.
