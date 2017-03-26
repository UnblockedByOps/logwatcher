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

import time
import os
import re
import sys
import socket

# FIXME: This needs to be automated and become graphtie compatible
CODE_VERSION = "$Id: gmetriclib.py 173790 2012-06-29 23:00:44Z wil $"


class gMetric:

    def __init__(self,
                 metric_type,
                 name,
                 units,
                 notused1,
                 graphite_server=None,
                 graphite_port=None,
                 metric_format=None,
                 debug=0):

        self.metric_type = metric_type
        self.metric_format = metric_format
        self.name = "%s.%s" % (self.gen_metric_path(), name)
        self.units = units
        # FIXME: Guessing this is for compatability with gmond gMetric
        self.maximum = notused1
        self.debug = debug # 0 == send, 1 == print, 2 == print+send
        self.__buffer = ""
        self.server = graphite_server
        self.port = graphite_port


    def gen_metric_path(self):
        '''Format graphite metric path based on hostname.'''

        hostname = socket.gethostname()
        fqdn = hostname.replace('.', '_')

        if self.metric_format == 'trp':
            cluster = hostname[:9].upper()
            datacenter = hostname[13:17].upper()
            resp = 'metrics.{0}-{1}.{2}'.format(cluster, datacenter, fqdn)

        else:
            ct_class = fqdn[7:10]
            resp = 'servers.{0}.{1}'.format(ct_class, fqdn)

        return resp

    def send(self, value, unused=0, autocommit=False):
        message = "%s %s %s" % (self.name, value, int(time.time()))
        if self.debug:
            print "send(%s)" % message

        self.__buffer += message

        if autocommit:
            self.commit()

    def pop(self):
        ret = self.__buffer
        self.__buffer = ""
        return ret

    def commit(self):
        if self.debug:
            print self.__buffer
        if self.debug == 1:
            self.__buffer = ""
            return True

        if sendMetrics(self.__buffer, self.server, self.port):
            self.__buffer = ""


def sendMetrics(data, server, port):

    print "SENDING: %s" % data

    try:
        sock = socket.socket()
        sock.connect((server, int(port)))
    except Exception as ex:
        print >> sys.stderr, "Failed to connect to %s:%s! (%s)" % (server,
                                                                   port,
                                                                   ex)
        return False

    try:
        sock.sendall(data+"\n")
    except Exception, e:
        print >> sys.stderr, "Failed to send data to %s:%s! (%s)" % (server, port, e)
        print >> sys.stderr, data

    try:
        sock.close()
        return True
    except:
        print >> sys.stderr, "Failed to close socket"
    return False
