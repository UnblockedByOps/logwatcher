'''Common functions.'''
#
#  Copyright 2015 CityGrid Media, LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import socket
import logging
import ConfigParser

LOG = logging.getLogger(__name__)

def send_to_graphite(data, server, port):
    '''Send a buffer of messages to graphite.'''

    for metric in [y for y in (x.strip() for x in data.splitlines()) if y]:
        LOG.info('SENDING: {0}'.format(metric))

    try:
        sock = socket.socket()
        sock.connect((server, int(port)))
    except Exception as ex:
        LOG.error('Failed to connect to {0}:{1}! ({2})'.format(server,
                                                               port,
                                                               ex))
        return False

    try:
        sock.sendall(data+"\n")
    except Exception as ex:
        LOG.error('Failed to send data to {0}:{1}! ({2})'.format(server,
                                                                 port,
                                                                 ex))

    try:
        sock.close()
        return True
    except Exception as ex:
        LOG.error('Failed to close socket: {0}'.format(ex))
    return False

def read_graphite_conf(conf='/etc/graphite.conf'):
    '''Read graphite config file and return graphite server name if found, None otherwise.'''

    LOG.debug('readGraphiteConf() called')
    sec = "graphite"

    try:
        parser = ConfigParser.ConfigParser()
        parser.read(conf)
        graphite_server = parser.get(sec, "server")

        return graphite_server

    except Exception as ex:
        LOG.error("Failed to read {0} ({1})".format(conf, ex))

    return None
