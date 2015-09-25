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

class test_plugin:
	def __init__(self, debug=False, config={}):
		self.lines_processed = 0
		self.bytes_processed = 0
	
	def process_line(self, line):
		if line == "99":
			raise Exception("Oh no, 99!")
		size = len(line)
		self.lines_processed += 1
		self.bytes_processed += size
		print "Line length: %d" % (size)
		return line

	def get_metrics(self):
		ret = {"lines_processed": self.lines_processed,
				"bytes_processed": self.bytes_processed}
		self.lines_processed = 0
		self.bytes_processed = 0
		return ret

