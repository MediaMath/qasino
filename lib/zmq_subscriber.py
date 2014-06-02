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

from txzmq import ZmqFactory, ZmqEndpoint, ZmqEndpointType, ZmqSubConnection

import logging

import json

from util import Identity

class ZmqSubscriber(ZmqSubConnection):

    def __init__(self, remote_host, port, zmq_factory, data_manager=None):

        self.data_manager = data_manager
        self.remote_host = remote_host
        self.generation_signal_listeners = []

        endpoint = ZmqEndpoint(ZmqEndpointType.connect, "tcp://%s:%d" % (remote_host, port))

        ZmqSubConnection.__init__(self, zmq_factory, endpoint)

    def shutdown(self):
        self.unsubscribe("GENSIG")
        ZmqSubConnection.shutdown(self)

    def subscribe_generation_signal(self, callback, *args):

        self.generation_signal_listeners.append( (callback, args) )
        self.subscribe("GENSIG")

    def gotMessage(self, message, tag):
        
        if tag == "GENSIG":
            logging.info("Received generation signal!")
            for callback, args in self.generation_signal_listeners:
                callback(*args)
