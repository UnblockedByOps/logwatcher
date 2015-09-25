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

import time,os,re,sys

code_version="$Id: gmetriclib.py 173790 2012-06-29 23:00:44Z wil $";

class gMetric:
	def __init__(self, type, name, units, max, mcast=None, debug=0):
		self.bin="/usr/bin/gmetric"
		self.type=type
		self.name=name
		self.units=units
		self.max=max
		self.mcast=mcast
		self.mcast_if=""
		self.debug=debug

		self.version=2

		if mcast==None:
			self.getChannel()

	def send(self, value, float=0):
		if float:
			value="%.3f" % value
		cmd_v2="%s --type=%s --name=%s --value=%s --units=%s --tmax=%s --mcast_channel=%s %s" % (self.bin, self.type, self.name, value, self.units, self.max, self.mcast, self.mcast_if)
		cmd_v3="%s -c /etc/gmond.conf --type=%s --name=%s --value=%s --units=%s --tmax=%s" % (self.bin, self.type, self.name, value, self.units, self.max )
		if self.version==2:
			cmd=cmd_v2
		else:
			cmd=cmd_v3

		if self.debug == 1:
			ret=0
			print cmd
		else:
			if self.debug==2:
				print cmd
			ret=os.system(cmd)

		if ret != 0:
			print "ERROR running "+cmd
			print "ERROR switching to ganglia version 3"
			if self.version==2:
				self.version=3
				if self.debug == 1:
					print cmd_v3
				ret=os.system(cmd_v3)
				if ret !=0:
					print "ERROR version 3 fails as well!"
				else:
					print "INFO version 3 works"

	def getChannel(self):
		conf="/etc/gmond.conf"
		if os.path.exists(conf):
			regex=re.compile("^mcast_channel\s+([\d.]+)")
			regex2=re.compile("^mcast_if\s+(\w+)")
			try:
				conf_fd=open(conf, 'r')
				lines=conf_fd.readlines()
				for line in lines:
					m=regex.search(line)
					if m:
						self.mcast=m.group(1)
					m=regex2.search(line)
					if m:
						self.mcast_if="--mcast_if=%s" % m.group(1)
				conf_fd.close()
				return 1
			except:
				print "ERROR: Couldn't find mcast_channel in %s" % conf
				sys.exit(9)
		else:
			print "ERROR: %s does not exist" % conf
			sys.exit(9)

