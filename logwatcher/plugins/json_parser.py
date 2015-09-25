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

import re
import socket
import json

class json_parser:
	def __init__(self, debug=False, config={}):
		self.lines_processed = 0
		self.logre=re.compile("^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"([A-Z]+) (.+?) HTTP/(\\d\.\\d)\" (\\d{3}) (\\S+) \"(.*)\" \"(.*)\" \"(\[.+?\])\" (\\d+)")
		self.customre=re.compile("\[([^[]+)=([^[]+)\]")
		self.hostname = socket.gethostname()
		self.fields = ["ip", "host", "user", "date", "method", "path", "ver", "rc", "bytes", "referer", "ua", "custom", "rt"]
		self.int_fields = ["rc", "bytes", "rt"]

	
	def process_line(self, line):
		self.lines_processed += 1

		ld = self.buildLogDict(line)
		if not ld:
			print >> sys.stderr, "FAILED TO PARSE %s" % line.strip()
			return line

		ld['server'] = self.hostname
		try:
			ld[self.fields[11]] = self.buildCustom(ld[self.fields[11]])
		except Exception, e:
			print >> sys.stderr, "FAILED TO PARSE %s" % line.strip()
			return line
		print json.dumps(ld)
		return line


	def get_metrics(self):
		ret = {"lines_processed": self.lines_processed}
		self.lines_processed = 0
		return ret


	def buildLogDict(self, line):
		m = self.logre.match(line)
		ret = {}
		if not m:
			return ret
		data = m.groups()
		for i,v in enumerate(data):
			if v == '-':
				ret[self.fields[i]] = None
			elif self.fields[i] in self.int_fields:
				ret[self.fields[i]] = int(v)
			else:
				ret[self.fields[i]] = v
		return ret


	def buildCustom(self, custom):
		return dict(self.customre.findall(custom))
