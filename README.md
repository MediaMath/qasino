Qasino
==========

Qasino is a stats collection system that supports tabular stats
stored in a database for querying with SQL.  Stats are ephemeral,
meaning only the latest stats reported for a given table are available
in the database.

Currently qasino is implemented in python using the twisted
framework and a zeromq transport.  The data store is sqlite.
