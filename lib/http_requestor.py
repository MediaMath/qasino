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

import requests
from util import Identity

class HttpRequestor():

    def __init__(self, hostname, port, username=None, password=None, skip_ssl_verify=False, url_proto='https'):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.skip_ssl_verify = skip_ssl_verify
        self.url_proto = url_proto
        self.conn = requests.Session()

    def send_table(self, table):

        request_options = { 'headers' : {'Content-Type': 'application/json'} }

        if self.skip_ssl_verify:
            request_options['verify'] = False
        if self.username and self.password:
            request_options['auth'] = (self.username, self.password)

        url = '{}://{}:{}/request?op=add_table_data'.format(self.url_proto, self.hostname, self.port)

        jsondata = table.get_json(op="add_table_data", identity=Identity.get_identity())

        #print jsondata
    
        request_options['data'] = jsondata

        try:
            response = self.conn.post(url, **request_options)
            response.raise_for_status()
        except Exception as e:
            return "ERROR: HttpRequestor: Request failed: url={}: {}".format(url, e)

        return None
