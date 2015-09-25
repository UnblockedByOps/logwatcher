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
# This plugin requires kafka-python and possibly msgpack-python (pip)
import re
import socket
import json
import sys
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

class kafka_logger:
	def __init__(self, debug=False, config={}):
		self.debug = config
		self.config = config

		self.send_when_debug = False
		if 'send_when_debug' in config:
			if config['send_when_debug'] in ['true', 'True', "On", "1"]:
				self.send_when_debug = config['send_when_debug']

		self.parse_version = 0.1

		self.lines_processed = 0
		self.kafka_sent = 0 # attempts, actually
		self.kafka_failed = 0
		self.kafka_errors_every = 100
		self.logre=re.compile("^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"([A-Z]+) ([^\?]+)\??(.+)? HTTP/(\\d\.\\d)\" (\\d{3}) (\\S+) \"(.*)\" \"(.*)\" \"(\[.+?\])\" (\\d+)")
		self.customre=re.compile("\[([^[]+)=([^[]+)\]")
		self.hostname = socket.gethostname()

		if "%s" in self.config['topic']:
			self.topic = self.config['topic'] % self.hostname[4:10]
		else:
			self.topic = self.config['topic']

		self.fields = ["ip", "host", "user", "date", "method", "path", "query_string", "http_version", "rc", "bytes", "referer", "ua", "custom", "rt"]
		self.int_fields = ["rc", "bytes", "rt"]

		try:
			self.kafka_client = KafkaClient(self.config['broker'])
			self.producer = SimpleProducer(self.kafka_client)
		except Exception, e:
			print >> sys.stderr, "ERROR: Failed to connect to kafka %s (%s)" % (Exception, e)
			self.producer = None

	
	def process_line(self, line):
		self.lines_processed += 1

		ld = self.buildLogDict(line)
		if not ld:
			print >> sys.stderr, "FAILED TO PARSE %s" % line.strip()
			return line

		ld['server'] = self.hostname
		ld['parse_version'] = self.parse_version
		try:
			ld['custom'] = self.buildCustom(ld['custom'])
		except Exception, e:
			print >> sys.stderr, "FAILED TO PARSE %s" % line.strip()
			return line
		self.sendKafka(json.dumps(ld))
		return line


	def get_metrics(self):
		if self.kafka_sent:
			frate = float(self.kafka_failed) / float(self.kafka_sent)
		ret = {"lines_processed": self.lines_processed, "kafka_sent": self.kafka_sent, "kafka_failed": self.kafka_failed, "kafka_failure_rate": frate}
		self.lines_processed = 0
		self.kafka_sent = 0
		self.kafka_failed = 0
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


	def sendKafka(self, json_line):
		self.kafka_sent += 1
		try:
			if self.send_when_debug or not self.debug:
				self.producer.send_messages(self.topic, json_line)
			return True
		except Exception, e:
			self.kafka_failed += 1
			if self.kafka_failed % self.kafka_errors_every == 1:
				print >> sys.stderr, "Failed to send %d messages to Kafka %s (%s)" % (self.kafka_failed, Exception, e)
			return False

