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

code_version="$Id: gmetriclib.py 173790 2012-06-29 23:00:44Z wil $";


class gMetric:
	def __init__(self, type, name, units, notused1, graphite_server="bogus-dev.relay-aws.graphite.ctgrd.com", debug=0):
		self.type=type
		self.name="%s.%s" % (self.getServerPrefix(), name)
		self.units=units
		self.max=max
		self.debug=debug # 0 == send, 1 == print, 2 == print+send
		self.__buffer = ""
		self.server = graphite_server
		self.port = 2003


	def getMetricPath(self):
		fqdn = socket.getfqdn()
		ct_class = fqdn[7:10]
		return "servers.%s.%s" % (ct_class, fqdn.replace('.', '_'))


	def getServerPrefix(self):
		hostname = socket.gethostname()
		fqdn = hostname.replace('.', '_')

		ct_class=fqdn[7:10]

		return "servers.%s.%s" % (ct_class, fqdn)



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


def sendMetrics(data, server, port=2003):
		print "SENDING: %s" % data
		try:
			sock = socket.socket()
			sock.connect((server, port))
		except Exception, e:
			print >> sys.stderr, "Failed to connect to %s:%s! (%s)" % (server, port, e)
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

