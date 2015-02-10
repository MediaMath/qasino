#!/usr/bin/python

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

import os
import sys
import signal
import logging
from optparse import OptionParser
import crypt

from twisted.internet import reactor, ssl, task
from twisted.application.internet import TCPServer
from twisted.application.service import Application
from twisted.web import server, resource, http, guard, static
from twisted.web.util import redirectTo
from twisted.python import log
from OpenSSL import SSL
from twisted.cred.portal import Portal, IRealm
from twisted.cred.checkers import FilePasswordDB

from zope.interface import implements

for path in [
    os.path.join('opt', 'qasino', 'lib'),
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib'))
]:
    if os.path.exists(os.path.join(path, '__init__.py')):
        sys.path.insert(0, path)
        break

from txzmq import ZmqFactory

import sql_receiver
import data_manager
import zmq_receiver
import zmq_requestor
import http_receiver
import http_receiver_ui
import zmq_publisher
import constants
import util

def signal_handler(signum, frame):
    sig_names = dict((k, v) for v, k in signal.__dict__.iteritems() if v.startswith('SIG'))
    logging.info("Caught %s.  Exiting...", sig_names[signum])
    if data_manager:
        data_manager.shutdown()
    reactor.stop()

if __name__ == "__main__":

    logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.INFO)

    parser = OptionParser()

    parser.add_option("-i", "--identity", dest="identity",
                      help="Use IDENTITY as identity", metavar="IDENTITY")
    parser.add_option("-f", "--db-file", dest="db_file", 
                      help="Use FILE as the sqlite database", metavar="FILE")
    parser.add_option("-d", "--db-dir", dest="db_dir", default="/ramdisk/qasino/dbs",
                      help="Use DIR as the sqlite database", metavar="DIR")
    parser.add_option("-k", "--archive-db-dir", dest="archive_db_dir",
                      help="Save database files to DIR after finished (otherwise they are deleted).", metavar="DIR")
    parser.add_option("-g", "--generation-duration", dest="generation_duration_s", default=30,
                      help="The length of a collection interval (generation) in seconds.", metavar="SECONDS")
    parser.add_option("-v", "--views-file", dest="views_file", default='views.conf',
                      help="A file containing a list of views to create.", metavar="FILE")
    parser.add_option("-K", "--keys-dir", dest="keys_dir", default='/opt/qasino/etc/keys/',
                      help="Directory where server keys can be found.", metavar="DIR")
    parser.add_option("-p", "--htpasswd-file", dest="htpasswd_file", default='/opt/qasino/etc/htpasswd',
                      help="Path to htpasswd file.", metavar="FILE")
    parser.add_option("-s", "--static-content-dir", dest="static_content_dir", default='/opt/qasino/etc/htdocs/static',
                      help="Path to static content dir.", metavar="DIR")
    parser.add_option("-t", "--templates-dir", dest="templates_dir", default='/opt/qasino/etc/htdocs/templates',
                      help="Path to template dir.", metavar="DIR")

    (options, args) = parser.parse_args()

    logging.info("Qasino server starting")

    if options.identity != None:
        util.Identity.set_identity(options.identity)

    logging.info("Identity is %s", util.Identity.get_identity())

    if not os.path.exists(options.db_dir):
        logging.info("Making directory: %s", options.db_dir)
        os.makedirs(options.db_dir)

    # Catch signals

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # For verbose adbapi logging...
    ##log.startLogging(sys.stdout)
    
    # Create a ZMQ factory

    zmq_factory = ZmqFactory()

    # Create a Pub/sub channel to blast out new generation signals.

    logging.info("Listening for ZeroMQ pub/sub clients on port %d.", constants.ZMQ_PUBSUB_PORT)
    
    zmq_publisher = zmq_publisher.ZmqPublisher(zmq_factory, constants.ZMQ_PUBSUB_PORT, data_manager)


    # Create a Data Manager instance that changes the sql backend's
    # pointers for which db is queried and which db is updated.

    data_manager = data_manager.DataManager(options.db_file, db_dir=options.db_dir, 
                                            signal_channel=zmq_publisher, archive_db_dir=options.archive_db_dir,
                                            generation_duration_s=options.generation_duration_s)

    def reread_views(views_file):
        try:
            mtime = os.path.getmtime(views_file)
        except:
            return

        if reread_views.last_mtime < mtime:
            logging.info("Reading views file '%s'.", views_file)
            reread_views.last_mtime = mtime
            data_manager.read_views(views_file)

    reread_views.last_mtime = 0

    if options.views_file != None:
        #data_manager.read_views(options.views_file)
        reread_views_task = task.LoopingCall(reread_views, options.views_file)
        reread_views_task.start(10.0)


    # Open a lister to receiver SQL queries.

    logging.info("Listening for SQL queries on port %d", constants.SQL_PORT)

    reactor.listenTCP(constants.SQL_PORT, sql_receiver.SqlReceiverFactory(data_manager))

    
    # Create a listener for responding to http requests.

    logging.info("Listening for HTTP requests on port %d", constants.HTTP_PORT)

    http_root = resource.Resource()
    http_root.putChild("request", http_receiver.HttpReceiver(data_manager))

    http_root.putChild("static",  static.File(options.static_content_dir))

    http_root.putChild("",        http_receiver_ui.UIResourceTables(options.templates_dir, data_manager))
    http_root.putChild("tables",  http_receiver_ui.UIResourceTables(options.templates_dir, data_manager))
    http_root.putChild("desc",    http_receiver_ui.UIResourceDesc(options.templates_dir, data_manager))
    http_root.putChild("query",   http_receiver_ui.UIResourceQuery(options.templates_dir, data_manager))

    http.HTTPFactory.protocol = http_receiver.MyLoggingHTTPChannel
    site = server.Site(http_root)

    reactor.listenTCP(constants.HTTP_PORT, site)

    logging.info("Listening for HTTPS requests on port %d", constants.HTTPS_PORT)

    class SimpleRealm(object):
        """
        A realm which gives out L{GuardedResource} instances for authenticated
        users.
        """
        implements(IRealm)

        def requestAvatar(self, avatarId, mind, *interfaces):
            if resource.IResource in interfaces:
                return resource.IResource, http_root, lambda: None
            raise NotImplementedError()

    def cmp_pass(uname, password, storedpass):
        sizeof_hash = len(storedpass)
        if sizeof_hash == 13:
            return crypt.crypt(password, storedpass[:2])
        else:
            return util.get_apache_md5(password, storedpass)

    checkers = [ FilePasswordDB(options.htpasswd_file, hash=cmp_pass) ]

    wrapper = guard.HTTPAuthSessionWrapper( Portal(SimpleRealm(), checkers),
                                            [ guard.BasicCredentialFactory('qasino.com') ])

    ssl_site =  server.Site(wrapper)

    try:
        if not os.path.isfile(options.htpasswd_file):
            raise Exception("htpasswd file '%s' does not exist" % options.htpasswd_file)

        reactor.listenSSL(constants.HTTPS_PORT, 
                          ssl_site, 
                          ssl.DefaultOpenSSLContextFactory(options.keys_dir + 'server.key', 
                                                           options.keys_dir + 'server.crt')
                         )

    except Exception as e:
        logging.info("Failed to listen on SSL port %d, continuing anyway (%s).", constants.HTTPS_PORT, str(e))


    # If the http port isn't port 80 make port 80 redirect to SSL.
    if constants.HTTP_PORT != 80:
        class SimpleRedirect(resource.Resource):
            isLeaf = True

            def render_GET(self, request):
                return redirectTo('https://{}:{}'.format(request.getRequestHostname(), constants.HTTPS_PORT), request)

        try:
            reactor.listenTCP(80, server.Site(SimpleRedirect()))
            logging.info("Listening for HTTP requests on port 80 to redirect to 443")
        except Exception as e:
            logging.info("Warning: failed to listen for HTTP requests on port 80 to redirect to 443: {}".format(str(e)))


    # Create a listener for responding to ZeroMQ requests.

    logging.info("Listening for ZeroMQ rpc clients on port %d", constants.ZMQ_RPC_PORT)

    zmq_receiver = zmq_receiver.ZmqReceiver(constants.ZMQ_RPC_PORT, zmq_factory, data_manager)


    # For testing connect to ourselves...

#    zmq_requestor = zmq_requestor.ZmqRequestor('127.0.0.1', constants.ZMQ_RPC_PORT, zmq_factory, data_manager)

    # Request metadata at fixed intervals.

#    request_metadata_task = task.LoopingCall(zmq_requestor.request_metadata)
#    request_metadata_task.start(8.0)



    # Run the event loop

    reactor.run()

    logging.info("Qasino server exiting")
