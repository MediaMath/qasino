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

from txzmq import ZmqFactory, ZmqEndpoint, ZmqEndpointType, ZmqREQConnection

import logging

import json

from util import Identity

class ZmqRequestor(ZmqREQConnection):

    def __init__(self, remote_host, port, zmq_factory, data_manager=None):

        self.data_manager = data_manager

        self.remote_host = remote_host

        endpoint = ZmqEndpoint(ZmqEndpointType.connect, "tcp://%s:%d" % (remote_host, port))

        ZmqREQConnection.__init__(self, zmq_factory, endpoint)

    def request_metadata(self):
        msg = { "op" : "get_table_list", "identity" : Identity.get_identity() }
        #logging.info("ZmqRequestor: Requesting table list from %s.", self.remote_host)
        deferred = self.sendMsg(json.dumps(msg))
        deferred.callback = self.message_received

    def send_table(self, table):
        deferred = self.sendMsg(table.get_json(op="add_table_data", identity=Identity.get_identity()))
        deferred.callback = self.message_received
        

    def message_received(self, msg):
        response_meta = json.loads(msg[0])

        if response_meta == None or response_meta["response_op"] == None:
            logging.error("ZmqRequestor: bad message response received")
        elif response_meta["response_op"] == "tables_list":
            logging.info("ZmqRequestor: Table list response: %s", json.loads(msg[1]))
        elif response_meta["response_op"] == "ok":
            logging.info("ZmqRequestor: request OK")
        elif response_meta["response_op"] == "error":
            logging.info("ZmqRequestor: request ERROR: " + response_meta["error_message"])
        else:
            logging.error("ZmqRequestor: unknown response: ", response_meta)
