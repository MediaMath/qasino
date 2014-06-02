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

import apsw
import thread

def connect(file):
    conn = apsw.Connection(file)
    conn.setbusytimeout(1000) # 1 second
    return conn
    
class ApswConnection(object):
    """
    A wrapper for a apsw connection instance.

    The wrapper passes almost everything to the wrapped connection and so has
    the same API. However, the Connection knows about its pool and also
    handle reconnecting should when the real connection dies.
    """

    def __init__(self, pool):
        self._pool = pool
        self._connection = None
        self.reconnect()

    def close(self):
        # The way adbapi works right now means that closing a connection is
        # a really bad thing  as it leaves a dead connection associated with
        # a thread in the thread pool.
        # Really, I think closing a pooled connection should return it to the
        # pool but that's handled by the runWithConnection method already so,
        # rather than upsetting anyone by raising an exception, let's ignore
        # the request
        pass

    def rollback(self):
        # Do not call commit or rollback because they don't exist:
        # http://apidoc.apsw.googlecode.com/hg/dbapi.html#connection-objects
        # The user of this class will just execute BEGIN, COMMIT, ROLLBACK as needed.
        return

    def commit(self):
        # Do not call commit or rollback because they don't exist:
        # http://apidoc.apsw.googlecode.com/hg/dbapi.html#connection-objects
        # The user of this class will just execute BEGIN, COMMIT, ROLLBACK as needed.
        return

    def reconnect(self):
        if self._connection is not None:
            self._pool.disconnect(self._connection)
        self._connection = self._pool.connect()

    def __getattr__(self, name):
        return getattr(self._connection, name)

__all__ = ['ApswConnection']
