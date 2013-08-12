
from txzmq import ZmqFactory, ZmqEndpoint, ZmqEndpointType, ZmqSubConnection

import logging

import json

from util import Identity

class JsonSubscriber(ZmqSubConnection):

    def __init__(self, remote_host, port, zmq_factory, data_manager=None):

        self.data_manager = data_manager
        self.remote_host = remote_host
        self.generation_signal_listeners = []

        endpoint = ZmqEndpoint(ZmqEndpointType.connect, "tcp://%s:%d" % (remote_host, port))

        ZmqSubConnection.__init__(self, zmq_factory, endpoint)


    def subscribe_generation_signal(self, callback, *args):

        self.generation_signal_listeners.append( (callback, args) )
        self.subscribe("GENSIG")

    def gotMessage(self, message, tag):
        
        if tag == "GENSIG":
            logging.info("Received generation signal!")
            for callback, args in self.generation_signal_listeners:
                callback(*args)
