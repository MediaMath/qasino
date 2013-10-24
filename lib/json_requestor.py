
from txzmq import ZmqFactory, ZmqEndpoint, ZmqEndpointType, ZmqREQConnection

import logging

import json

from util import Identity

class JsonRequestor(ZmqREQConnection):

    def __init__(self, remote_host, port, zmq_factory, data_manager=None):

        self.data_manager = data_manager

        self.remote_host = remote_host

        endpoint = ZmqEndpoint(ZmqEndpointType.connect, "tcp://%s:%d" % (remote_host, port))

        ZmqREQConnection.__init__(self, zmq_factory, endpoint)

    def request_metadata(self):
        msg = { "op" : "get_table_list", "identity" : Identity.get_identity() }
        #logging.info("JsonRequestor: Requesting table list from %s.", self.remote_host)
        deferred = self.sendMsg(json.dumps(msg))
        deferred.callback = self.message_received

    def send_table(self, table, persist=None, identity=None, static=None):
        if not identity:
            identity = Identity.get_identity()
        msg = { "op" : "add_table_data",  "identity" : identity }
        if persist:
            msg["persist"] = persist

        if static:
            msg["static"] = static

        #logging.info("JsonRequestor: Sending table '%s'", table["tablename"])
        msg["table"] = table

        deferred = self.sendMsg(json.dumps(msg))
        deferred.callback = self.message_received
        

    def message_received(self, msg):
        response_meta = json.loads(msg[0])

        if response_meta == None or response_meta["response_op"] == None:
            logging.error("JsonRequestor: bad message response received")
        elif response_meta["response_op"] == "tables_list":
            logging.info("JsonRequestor: Table list response: %s", json.loads(msg[1]))
        elif response_meta["response_op"] == "ok":
            logging.info("JsonRequestor: request OK")
        else:
            logging.error("JsonRequestor: unknown response: ", response_meta)
