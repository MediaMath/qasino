
from txzmq import ZmqFactory, ZmqEndpoint, ZmqEndpointType, ZmqPubConnection

import logging

import json

from util import Identity

class JsonPublisher(ZmqPubConnection):

    def __init__(self, zmq_factory, port, data_manager=None):

        self.data_manager = data_manager

        endpoint = ZmqEndpoint(ZmqEndpointType.bind, "tcp://*:%d" % port)

        ZmqPubConnection.__init__(self, zmq_factory, endpoint)


    def send_generation_signal(self, generation_number, generation_duration_s):
        msg = { "op" : "generation_signal", "identity" : Identity.get_identity(), "generation_number" : generation_number }
        if generation_duration_s:
            msg["generation_duration_s"] = generation_duration_s

        self.publish(json.dumps(msg), "GENSIG")

