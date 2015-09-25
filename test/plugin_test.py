#!/usr/bin/python

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

import sys

sys.path.append("../plugins")


plugin_list = ["test_plugin", "echo"]
plugins = []

for p in plugin_list:
	m = __import__(p)
	c = getattr(m,p)
	plugins.append(c())

print plugins

l=0
while l<2:
	l+=1
	i=0
	while i<100:
		i+=1
		for p in plugins:
			try:
				p.process_line("%d" % i)
			except Exception, e:
				print >> sys.stderr, "Error processing line '%s': %s (%s)" % (i, Exception, e)
	for p in plugins:
		print "%s: %s" % (p.__class__.__name__, p.get_metrics())

