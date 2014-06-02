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

from txzmq import ZmqFactory, ZmqEndpoint, ZmqEndpointType, ZmqPubConnection

import logging

import json

from util import Identity

class ZmqPublisher(ZmqPubConnection):

    def __init__(self, zmq_factory, port, data_manager=None):

        self.data_manager = data_manager

        endpoint = ZmqEndpoint(ZmqEndpointType.bind, "tcp://*:%d" % port)

        ZmqPubConnection.__init__(self, zmq_factory, endpoint)


    def send_generation_signal(self, generation_number, generation_duration_s):
        msg = { "op" : "generation_signal", "identity" : Identity.get_identity(), "generation_number" : generation_number }
        if generation_duration_s:
            msg["generation_duration_s"] = generation_duration_s

        self.publish(json.dumps(msg), "GENSIG")

