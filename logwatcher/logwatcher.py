#!/bin/env python
# !/usr/local/bin/python

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

import time,os,re,sys,atexit,ConfigParser
import signal

code_version="$Id: logwatcher.py 233274 2014-06-23 23:20:52Z heitritterw $";

class LogWatcher:
	def __init__(self, pidfile=None, daemonize=0, configfile=None, distinguisher=None, debug=0, quit=False, beginning=False, testconfig=False, graphite_server=None, use_graphite=False):
		self.log=""
		self.fd=None

		self.graphite_server=graphite_server
		if self.graphite_server:
			self.use_graphite = True
		else:
			self.use_graphite = use_graphite

		# initializing, will be populated later
		self.plugin_list=[]
		self.plugins=[]
		self.plugin_dir=None
		self.plugin_paths = ["/app/logwatcher/plugins", os.path.dirname(__file__)+"/plugins"]
		self.gmetric_brands={}
		self.regex={}
		self.gmetric={}
		# metrics that count matching lines
		self.metric_counts={}
		# metrics that sum values found
		self.metric_sums={}
		# metrics that are calculated from other metrics
		self.metric_calcs={}
		self.metric_calc_expr={}

		# metrics that describe distributions
		self.metric_dists={}
		self.metric_dist_bucketsize={}
		self.metric_dist_bucketcount={}

		self.ignore_pattern=""
		self.ignore=None

		self.configfile=configfile

		self.debug=debug
		self.pidfile=pidfile
		self.distinguisher=distinguisher
		self.quit=quit
		self.beginning=beginning
		self.testconfig=testconfig

		self.log_time=0
		self.log_time_start=0
		self.notify_time=0
		self.notify_time_start=0

		self.readConfig()
		signal.signal(signal.SIGHUP, self.reReadConfig)

		self.new_metric_count=0 # counts new-found dynamic metrics
		self.total_metric_count=0 # counts metrics sent

		self.prefix_root="LW_"
		self.prefix=self.prefix_root
		if self.distinguisher:
			self.prefix="%s%s_" % (self.prefix, self.distinguisher)

		self.daemonize=daemonize

		if self.getPID() < 1:
			if self.daemonize == 1:
				procdaemonize()

		if self.lockPID() == 0:
			print "Pidfile found"
			sys.exit(-1)

		self.log_count=0 # how many different logs have we opened?
		self.curr_pos=0
		self.prev_pos=0
		self.last_time=time.time()

		if self.use_graphite and not self.graphite_server:
			self.graphite_server = self.readGraphiteConf()
			if not self.graphite_server:
				print >> sys.stderr, "ERROR: Failed to set graphite server. Using gmetric."
			else:
				self.use_graphite = True

		self.brand_counts={}

		if self.graphite_server:
			from graphitelib import gMetric
		else:
			from gmetriclib import gMetric

		self.gmetric["Q"]=gMetric("float", "%sQueries" % self.prefix, "count", self.notify_schedule,self.graphite_server,self.debug)
		self.gmetric["QPS"]=gMetric("float", "%sQPS" % self.prefix, "qps", self.notify_schedule,self.graphite_server,self.debug)
		self.gmetric["APT"]=gMetric("float", "%sAvg_Processing_Time" % self.prefix, "seconds", self.notify_schedule,self.graphite_server,self.debug)
		self.gmetric["MAX"]=gMetric("float", "%sMax_Processing_Time" % self.prefix, "seconds", self.notify_schedule,self.graphite_server,self.debug)
		self.gmetric["TPT"]=gMetric("float", "%sTotal_Processing_Time" % self.prefix, "seconds", self.notify_schedule,self.graphite_server,self.debug)
		self.gmetric["SLA"]=gMetric("float", "%sexceeding_SLA" % self.prefix, "percent", self.notify_schedule,self.graphite_server,self.debug)
		self.gmetric["SLA_ct"]=gMetric("float", "%sexceeding_SLA_ct" % self.prefix, "percent", self.notify_schedule,self.graphite_server,self.debug)
		self.gmetric["code_version"]=gMetric("string", "%sLW_Version" % self.prefix_root, "string", self.notify_schedule,self.graphite_server,self.debug)
		self.gmetric["ignore"]=gMetric("float", "%signored" % self.prefix, "count", self.notify_schedule,self.graphite_server,self.debug)

		self.gmetric["NOTIFY_TIME"]=gMetric("float", "%s%s" % (self.prefix_root,"LW_NotifyTime"), "seconds", self.notify_schedule,self.graphite_server,self.debug)
		self.gmetric["LOG_TIME"]=gMetric("float", "%s%s" % (self.prefix_root,"LW_LogTime"), "seconds", self.notify_schedule,self.graphite_server,self.debug)
		self.gmetric["NEW_METRICS"]=gMetric("float", "%s%s" % (self.prefix_root,"LW_NewMetrics"), "float", self.notify_schedule,self.graphite_server,self.debug)
		self.gmetric["TOTAL_METRICS"]=gMetric("float", "%s%s" % (self.prefix_root,"LW_TotalMetrics"), "float", self.notify_schedule,self.graphite_server,self.debug)

		# use this for sub-hourly and other odd log rotation
		self.curr_inode=None

		self.prime_metrics()

		self.initialize_counters()
		self.watch()

	def readTestConfig(self):
		sec="test"

		if self.configfile == None:
			return 0
		try:
			cp = ConfigParser.ConfigParser()
			cp.read(self.configfile)
			self.logformat=cp.get(sec, "log_name_format")
		except:
			pass

		try:
			self.notify_schedule=int(cp.get(sec, "notify_schedule"))
		except:
			pass

	def reReadConfig(self,signum,frame):
		self.readConfig()

	def readConfig(self):
		if self.debug:
			print >> sys.stderr, "DEBUG: readconfig() called"
		sec="logwatcher"

		if self.configfile == None:
			return 0
		try:
			cp = ConfigParser.ConfigParser()
			cp.read(self.configfile)
			self.logformat=cp.get(sec, "log_name_format")

			if not self.graphite_server:
				try:
					self.use_graphite=cp.getboolean(sec, "use_graphite")
				except:
					pass

			# "except -> pass" for those that come in via commandline
			if self.pidfile==None:
				try:
					self.pidfile=cp.get(sec, "pidfile")
				except:
					pass

			if not self.plugin_dir:
				try:
					self.plugin_list=cp.get(sec, "plugin_dir")
				except:
					pass

			if self.plugin_dir:
				if os.path.exists(self.plugin_dir):
					sys.path.append(self.plugin_dir)
				else:
					print >> sys.stderr, "ERROR: %s does not exist" % self.plugin_dir
			else:
				for pp in self.plugin_paths:
					if os.path.exists(pp):
						sys.path.append(pp)
						break

			if not self.plugin_list:
				try:
					self.plugin_list=cp.get(sec, "plugins").split()
				except:
					pass

			print >> sys.stderr, "Loading plugins: %s" % self.plugin_list
			try:
				for plugin in self.plugin_list:
					print >> sys.stderr, "Loading plugin: %s" % (plugin)
					mod = __import__(plugin)   # import the module
					cls = getattr(mod, plugin) # name the class so we can call it
					self.plugins.append(cls(self.debug, self.getPluginConf(plugin))) # create an instance of the class
			except Exception, e:
				print >> sys.stderr, "Failed to load plugin: %s (%s)" % (Exception, e)
				sys.exit(4) # should it be this serious?

			import string
			self.sla=float(cp.get(sec, "sla_ms"))/1000.0 # self.sla is seconds

			try:
				self.nologsleep=int(cp.get(sec, "nologsleep"))
			except:
				self.nologsleep=10

			try:
				self.notify_schedule=int(cp.get(sec, "notify_schedule"))
			except:
				self.notify_schedule=60

			try:
				self.debug=int(cp.get(sec, "debug"))
			except:
				pass

			#print "DEBUG: %d" % self.notify_schedule
			self.regex["processing_time"] = re.compile(cp.get(sec, "processing_time_regex"))
			self.processing_time_units=cp.get(sec, "processing_time_units")

			self.use_brand=0
			try:
				use_brand=int(cp.get(sec, "use_brand"))
				if use_brand == 1:
					self.use_brand=1
			except:
				pass

			if self.use_brand == 1:
				self.regex["brand"] = re.compile(cp.get(sec, "brand_regex"))

			if self.distinguisher==None:
				try:
					self.distinguisher=cp.get(sec, "distinguisher")
				except:
					pass

			# read in the metrics to prime
			try:
				self.metrics_prime_list=cp.get(sec, "metrics_prime").split(" ")
			except:
				self.metrics_prime_list=()

			# read in the Count metrics, and optionally, the ratio metrics
			self.metrics_count_list=cp.get(sec, "metrics_count").split(" ")
			try:
				self.metrics_ratio_list=cp.get(sec, "metrics_ratio").split(" ")
			except:
				self.metrics_ratio_list=()
			for metric in self.metrics_count_list:
				self.regex[metric]=re.compile(cp.get(sec, "metric_%s_regex" % metric))

			# read in the Sum metrics; these can be ratio metrics as well!
			try:
				self.metrics_sum_list=cp.get(sec, "metrics_sum").split(" ")
				to_remove=[]
				for metric in self.metrics_sum_list:
					try:
						self.regex[metric]=re.compile(cp.get(sec, "metric_%s_regex" % metric))
					except:
						print "ERROR: Failed to find metric_%s_regex!" % metric
						# remove it after we leave the loop
						to_remove.append(metric)
				for tr in to_remove:
					self.metrics_sum_list.remove(tr)
			except Exception, e:
				print "ERROR: error reading metrics_sum: %s" % e
				self.metrics_sum_list=()

			# read in the calc metrics
			try:
				self.metrics_calc_list=cp.get(sec, "metrics_calc").split(" ")
				for metric in self.metrics_calc_list:
					try:
						self.metric_calc_expr[metric]=cp.get(sec, "metric_%s_expression" % metric)
					except:
						print "ERROR: Failed to find metric_%s_regex!" % metric
						self.metrics_calc_list.remove(metric)
			except:
				self.metrics_calc_list=()

			# read in the distribution metrics
			try:
				self.metrics_dist_list=cp.get(sec, "metrics_dist").split(" ")
				for metric in self.metrics_dist_list:
					try:
						self.metric_dist_bucketsize[metric]=int(cp.get(sec, "metric_%s_bucket_size" % metric))
						self.metric_dist_bucketcount[metric]=int(cp.get(sec, "metric_%s_bucket_count" % metric))
						self.regex[metric]=re.compile(cp.get(sec, "metric_%s_regex" % metric))
					except Exception, e:
						print "ERROR: Failed to set up metric_%s_regex! (%s)" % (metric, e)
						self.metrics_dist_list.remove(metric)
			except:
				self.metrics_dist_list=()

			# Get the ignore pattern. We'll completely ignore (but count) any matching lines.
			try:
				self.ignore_pattern=cp.get(sec, "ignore_pattern")
			except:
				self.ignore_pattern="^$" # safe to ignore
			self.ignore=re.compile(self.ignore_pattern)

			# this will be used to cleanse "found" metric names
			try:
				self.metric_cleaner=re.compile(cp.get(sec, "metric_cleaner"))
			except:
				self.metric_cleaner=re.compile("[/.:;\"\' $=]")

			# STUB need some error handling for ratios that don't exist

		except Exception, e:
			print "failed to parse config file '%s'" % self.configfile
			print "The following options are required:"
			print " log_name_format"
			print " sla_ms"
			print " processing_time_regex"
			print " use_brand"
			print " brand_regex"
			print " metrics_count"
			print "    metric_<metric_name>_regex for any metric listed in metrics_count"
			print "Root error: %s" % e
			sys.exit(1)
		if self.testconfig:
			self.readTestConfig()


	def readGraphiteConf(self):
		conf = "/etc/graphite.conf"
		if self.debug:
			print >> sys.stderr, "DEBUG: readGraphiteConf() called"
		sec="graphite"

		try:
			cp = ConfigParser.ConfigParser()
			cp.read(conf)
			self.graphite_server=cp.get(sec, "server")
			return self.graphite_server
		except Exception, e:
			print "Failed to read %s (%s)" % (conf, e)
		return None


	def getPluginConf(self, plugin):
		if self.debug:
			print >> sys.stderr, "DEBUG: getPluginConf(%s) called" % plugin
		sec=plugin

		if self.configfile == None:
			return 0
		try:
			cp = ConfigParser.ConfigParser()
			cp.read(self.configfile)
			return dict(cp.items(plugin))
		except:
			return {}

	def lockPID(self):
		pid=self.getPID()
		if pid == -1: # not using pidfile
			return 1
		elif pid == 0: # no pidfile
			atexit.register(self.removePID)
			f = open(self.pidfile, "w")
			f.write("%d" % os.getpid())
			f.close()
			return 1
		else:
			print "PID is %d" % pid
			return 0

		if os.path.exists(self.pidfile):
			return 0

	def removePID(self):
		try:
			os.unlink(self.pidfile)
		except:
			print "unable to unlink pidfile!"

	def getPID(self):
		if not self.pidfile:
			return -1
		if os.path.exists(self.pidfile):
			f = open(self.pidfile)
			p = f.read()
			f.close()
			return int(p)
		else:
			return 0

	def prime_metrics(self):
		for pair in self.metrics_prime_list:
			try:
				pmetric,val = pair.split(":")
				m=gMetric("float", "%s%s" % (self.prefix,pmetric), "prime", self.notify_schedule,self.graphite_server,self.debug)
				m.send(float(val),1)
				self.total_metric_count += 1
			except Exception, e:
				print >> sys.stderr, "Failed to send prime metric %s (%s)" % (pair, e)

	def notifybrand(self, brand, seconds):
		if self.graphite_server:
			from graphitelib import gMetric
		else:
			from gmetriclib import gMetric

		try:
			if not self.gmetric_brands.has_key(brand):
				self.gmetric_brands[brand]=gMetric("float", "%sQPS_%s" % (self.prefix,brand), "qps", self.notify_schedule,self.graphite_server,self.debug)
			self.gmetric_brands[brand].send(float(self.brand_counts[brand]/seconds), 1)
			self.total_metric_count += 1
		except Exception, e:
			print "couldn't notify for brand %s (%s)" % (brand, e)

	def notify(self, seconds):
		if self.graphite_server:
			from graphitelib import gMetric
			from graphitelib import sendMetrics
		else:
			from gmetriclib import gMetric

		self.notify_time_start=time.time()
		#print time.strftime("%H:%M:%S")
		if self.pt_requests > 0:
			self.gmetric["TPT"].send(self.processing_time, 1)
			#print "%.2f / %d" % (self.processing_time,self.pt_requests)
			self.gmetric["APT"].send(self.processing_time/self.pt_requests, 1)
			self.gmetric["MAX"].send(self.max_processing_time, 1)
			self.gmetric["SLA"].send(self.pt_requests_exceeding_sla*100.0/self.pt_requests, 1)
			self.gmetric["SLA_ct"].send(self.pt_requests_exceeding_sla, 1)
		else:
			self.gmetric["TPT"].send(0.0, 1)
			self.gmetric["APT"].send(0.0, 1)
			self.gmetric["MAX"].send(0.0, 1)
			self.gmetric["SLA"].send(0.0, 1)
			self.gmetric["SLA_ct"].send(0.0, 1)
		if seconds > 0:
			qps=float(self.requests/seconds)
		else:
			qps=0.0
		self.gmetric["Q"].send(self.requests, 1)
		self.gmetric["QPS"].send(qps, 1)
		#print self.processing_time
		self.total_metric_count += 7

		#print "covered %d, requests %d" % (self.covered,self.requests)
		if self.requests > 0:
			coverage_per_query=self.covered*100.0/self.requests
		else:
			coverage_per_query=0.0
		#print "served %d, possible %d" % (self.inventory_served,self.inventory_possible)
		if self.inventory_possible > 0:
			coverage_per_ad_requested=self.inventory_served*100.0/self.inventory_possible
		else:
			coverage_per_ad_requested=0.0

		#self.gmetric_cpq.send(coverage_per_query, 1)
		#self.gmetric_cpar.send(coverage_per_ad_requested, 1)

		self.gmetric["code_version"].send("\"%s\"" % code_version, 0)
		self.gmetric["ignore"].send(self.ignored_count, 1)
		self.total_metric_count += 2

		for brand in self.brand_counts.keys():
			self.notifybrand(brand,seconds)

		for rmetric in self.metrics_ratio_list:
			tot=0
			regex=re.compile("^%s" % rmetric)
			for smetric in self.metric_sums.keys():
				rmetric_name="%s_ratio" % smetric
				if re.match(regex, smetric):
					if self.requests != 0:
						# we don't want to multiply by 100 for sum ratios
						perc=float(self.metric_sums[smetric])/float(self.requests)
					else:
						perc=0.0
					try:
						self.gmetric[rmetric_name].send(perc,1)
					except: #sketchy
						self.gmetric[rmetric_name]=gMetric("float", "%s%s" % (self.prefix,rmetric_name), "percent", self.notify_schedule,self.graphite_server,self.debug)
						self.gmetric[rmetric_name].send(perc,1)
					self.total_metric_count += 1
				
			for cmetric in self.metric_counts.keys():
				if re.match(regex, cmetric):
					tot=tot+self.metric_counts[cmetric]
					#print "TOTAL %d" % tot
			for cmetric in self.metric_counts.keys():
				rmetric_name="%s_ratio" % cmetric
				if re.match(regex, cmetric):
					if tot!=0:
						perc=float(self.metric_counts[cmetric])/float(tot) * 100
					else:
						perc=0.0
					#print "%s %s %.2f" % (self.metric_counts[cmetric], cmetric, perc)
					try:
						self.gmetric[rmetric_name].send(perc,1)
					except: #sketchy
						self.gmetric[rmetric_name]=gMetric("float", "%s%s" % (self.prefix,rmetric_name), "percent", self.notify_schedule,self.graphite_server,self.debug)
						self.gmetric[rmetric_name].send(perc,1)
					self.total_metric_count += 1

		# send smetrics
		for smetric in self.metric_sums.keys():
			#print "DEBUG: sending %.2f" % self.metric_sums[smetric]
			try:
				self.gmetric[smetric].send(self.metric_sums[smetric],1)
			except: #sketchy
				self.gmetric[smetric]=gMetric("float", "%s%s" % (self.prefix,smetric), "sum", self.notify_schedule,self.graphite_server,self.debug)
				self.gmetric[smetric].send(self.metric_sums[smetric],1)
			self.total_metric_count += 1

		# send cmetrics
		for cmetric in self.metric_counts.keys():
			#print "DEBUG: sending %.2f" % self.metric_counts[cmetric]
			try:
				self.gmetric[cmetric].send(self.metric_counts[cmetric],1)
			except: #sketchy
				self.gmetric[cmetric]=gMetric("float", "%s%s" % (self.prefix,cmetric), "count", self.notify_schedule,self.graphite_server,self.debug)
				self.gmetric[cmetric].send(self.metric_counts[cmetric],1)
			self.total_metric_count += 1

		# send emetrics/calcs
		for emetric in self.metric_calcs.keys():
			try:
				cvalue=self.calculate(self.metric_calc_expr[emetric])
			except Exception, e:
				print Exception, e
				cvalue=0
			#print "DEBUG: emetric sending %.2f for %s" % (cvalue, emetric)

			try:
				self.gmetric[emetric].send(cvalue,1)
			except Exception, e: #sketchy, create then send instead of pre-initializing
				self.gmetric[emetric]=gMetric("float", "%s%s" % (self.prefix,emetric), "expression", self.notify_schedule,self.graphite_server,self.debug)
				self.gmetric[emetric].send(cvalue,1)
			self.total_metric_count += 1

		# send dmetrics
		for dmetric in self.metric_dists.keys():
			regex=re.compile("^%s" % rmetric)

			# Let's do the ratio metrics in-line here
			do_ratio=False
			if re.match(regex, dmetric):
				do_ratio=True

			last=0
			for bucket in range(self.metric_dist_bucketcount[dmetric]):
				current=last+self.metric_dist_bucketsize[dmetric]
				# first bucket
				if last == 0:
					dmetric_b="%s_%d-%d" % (dmetric, 0, current-1)
				# last bucket
				elif bucket == self.metric_dist_bucketcount[dmetric]-1:
					dmetric_b="%s_%d-%s" % (dmetric, last, "inf")
				# other buckets
				else:
					dmetric_b="%s_%d-%d" % (dmetric, last, current-1)
				last=current
				#print dmetric_b,self.metric_dists[dmetric][bucket]
				#print "DEBUG: sending %.2f" % self.metric_counts[dmetric_b][bucket]
				try:
					self.gmetric[dmetric_b].send(self.metric_counts[dmetric_b],1)
				except: #sketchy
					self.gmetric[dmetric_b]=gMetric("float", "%s%s" % (self.prefix,dmetric_b), "count", self.notify_schedule,self.graphite_server,self.debug)
					self.gmetric[dmetric_b].send(self.metric_dists[dmetric][bucket],1)
				self.total_metric_count += 1

				if self.requests != 0:
					# we don't want to multiply by 100 for sum ratios
					perc=float(self.metric_dists[dmetric][bucket])/float(self.requests) * 100
					#perc=float(self.metric_counts[cmetric])/float(tot) * 100 # do we need to count matches (tot)?
				else:
					perc=0.0
				try:
					self.gmetric[dmetric_b+"_ratio"].send(perc,1)
				except: #sketchy
					self.gmetric[dmetric_b+"_ratio"]=gMetric("float", "%s%s_ratio" % (self.prefix,dmetric_b), "percent", self.notify_schedule,self.graphite_server,self.debug)
					self.gmetric[dmetric_b+"_ratio"].send(perc,1)
				self.total_metric_count += 1

		# send plugin metrics
		for p in self.plugins:
			try:
				pmetrics = p.get_metrics()
			except Exception, e:
				print >> sys.stderr, "WARNING: %s.get_metrics() failed. (%s)" % (p.__class__.__name__, e)
				continue
			for pmetric in pmetrics.keys():
				pmn = "plugins.%s.%s" % (p.__class__.__name__, pmetric)
				try:
					self.gmetric[pmn].send(pmetrics[pmetric],1)
				except: #sketchy
	 				self.gmetric[pmn]=gMetric("float", "%s%s" % (self.prefix,pmn), "count", self.notify_schedule,self.graphite_server,self.debug)
					self.gmetric[pmn].send(pmetrics[pmetric],1)
				self.total_metric_count += 1

		self.gmetric["LOG_TIME"].send(self.log_time,1)
		self.gmetric["NEW_METRICS"].send(self.new_metric_count,1)
		self.total_metric_count += 3 # includes the next line
		self.gmetric["TOTAL_METRICS"].send(self.total_metric_count,1)
		if self.graphite_server:
			buffer = ""
			for m in self.gmetric:
				buffer += "%s\n" % self.gmetric[m].pop()
			for m in self.gmetric_brands:
				buffer += "%s\n" % self.gmetric_brands[m].pop()
			sendMetrics(buffer, self.graphite_server)

		# after sending batch, stop the timer
		self.notify_time=time.time() - self.notify_time_start

		# ...the one place where we changed the call for graphite
		if self.graphite_server:
			self.gmetric["NOTIFY_TIME"].send(self.notify_time,autocommit=True)
		else:
			self.gmetric["NOTIFY_TIME"].send(self.notify_time,1)


		if self.quit:
			print "Metrics complete."
			sys.exit(0)

		self.initialize_counters()

	def initialize_counters(self):
		# processing_time
		self.processing_time=0
		self.max_processing_time=0
		self.requests=0
		self.pt_requests=0
		self.pt_requests_exceeding_sla=0
		for brand in self.brand_counts.keys():
			self.brand_counts[brand]=0

		for cmetric in self.metric_counts.keys():
			self.metric_counts[cmetric]=0
		for smetric in self.metric_sums.keys():
			self.metric_sums[smetric]=0
		# this one is different, since the dict isn't created while reading the log
		for emetric in self.metrics_calc_list:
			self.metric_calcs[emetric]=0

		for dmetric in self.metrics_dist_list:
			self.metric_dists[dmetric]={}
			for bucket in range(self.metric_dist_bucketcount[dmetric]):
				self.metric_dists[dmetric][bucket]=0

		# coverage
		self.inventory_possible=0
		self.covered=0
		self.inventory_served=0
		self.ignored_count=0

		self.notify_time=0
		self.log_time=0
		self.new_metric_count=0
		self.total_metric_count=0

	def logbrand(self,brand,pt=None,coverate=None):
		if self.brand_counts.has_key(brand):
			self.brand_counts[brand]+=1
		else:
			self.brand_counts[brand]=1
			if self.debug:
				print >> sys.stderr, "DEBUG: Found new publisher: %s" % brand
			self.new_metric_count+=1

	def openlog(self):
		try:
			if self.fd:
				self.fd.close()
				if self.debug:
					print >> sys.stderr, "DEBUG: closing existing logfile"
		except:
			print "close() failed"
		try:
			self.fd=open(self.log, 'r')
			if self.debug:
				print >> sys.stderr, "DEBUG: opening logfile %s" % self.log
				print "DEBUG: log count = %d" % self.log_count
			# go to end of the log unless we override (w/beginning) or ARE in the first log
			if ((not self.beginning) and (self.log_count == 0)):
				self.fd.seek(0,2)
				if self.debug:
					print >> sys.stderr, "DEBUG: GOING TO THE END"
			self.log_count+=1
			self.curr_pos=self.prev_pos=self.fd.tell()
			self.curr_inode=os.stat(self.log)[1]
			if self.debug:
				print >> sys.stderr, "DEBUG: current position is %d" % self.curr_pos
		except Exception, e:
			print "Error in openlog(): "+str(e)
			sys.exit(9)
	def setlogname(self):
		nowfile=time.strftime(self.logformat)
		if nowfile == self.log:
			#print "existing log"
			# should return 1 if log filename changed OR if inode changed!
			try:
				filename_inode=os.stat(nowfile)[1]
				if self.curr_inode != filename_inode:
					return 1
			except Exception, e:
				# file probably renamed, but no new one yet
				pass
			return 0
		if os.path.exists(nowfile):
			if self.debug:
				print >> sys.stderr, "DEBUG: FOUND A NEW LOGFILE, we should switch (after finishing)"
			self.log=nowfile
			return 1
		return 0

	"""
	warning to sdterr
	"""
	def send_warning(self, msg):
		print >> sys.stderr, "WARNING: %s" % msg

	"""
	this will replace variables with values in an expression
	unknown items will be replaced with '_unknown_', forcing an exception at calculate() time
	"""
	def parse_expression(self, expression):
		nexpression=""
		try:
			for bit in expression.split(" "):
				try:
					value=float(bit)
				except:
					ValueError, TypeError
					if bit[:2] == "s/":
						try:
							value=self.metric_sums[bit[2:]]
						except:
							self.send_warning("in parse_expression() value for %s not found" % bit)
							value=0
					elif bit[:2] == "c/":
						try:
							value=self.metric_counts[bit[2:]]
						except:
							self.send_warning("in parse_expression() value for %s not found" % bit)
							print self.metric_counts.keys()
							value=0
					# allow any object property to be used
					elif bit[:2] == "i/":
						try:
							value=getattr(self,bit[2:])
						except:
							self.send_warning("in parse_expression() value for %s not found" % bit)
							value=0
					elif bit in ('/', '+', '-', '*'):
						value=bit
					else:
						value="_unknown_"
				nexpression="%s %s" % (nexpression, value)
		except Exception, e:
			print "Exception in parse_expression(): %s (%s)" % (Exception, e)
			nexpression="-1"
		return nexpression

	"""
	evaluate a parsed user-configured expression
	"""
	def calculate(self, expression):
		try:
			if self.debug:
				print >> sys.stderr, "calculate(%s)" % self.parse_expression(expression)
			value=eval(self.parse_expression(expression))
		except ZeroDivisionError, e:
			#print "Division by zero in calculate(%s)" % expression
			value=0
		except Exception, e:
			value=-1
			print >> sys.stderr, "Exception in calculate(): %s (expression: '%s')" % (e, expression)
		return value

	"""
	watch the log file for new lines
	"""
	def watch(self):
		# save_line is a buffer for saving a partial line at the end of a read
		save_line=""
		finish_counter = 0 # make sure we finished the previous file
		finish_tries = 3   # make sure we finished the previous file
		line = None
		while 1:
			now=time.time()
			if self.last_time+self.notify_schedule<=now:
				self.notify(now-self.last_time)
				self.last_time=now
			time.sleep(1)

			if self.setlogname() == 1:
				# we'll switch to the new log after trying the last log finish_tries times
				finish_counter += 1
				if self.debug:
					print >> sys.stderr, "DEBUG: Last line was %s (try %d)" % (line, finish_counter)
				if self.fd == None or finish_counter >= finish_tries:
					self.openlog()
					finish_counter = 0
			elif (self.fd == None):
				print "ERROR: logfile %s not opened, sleeping %ds" % (self.log, self.nologsleep)
				time.sleep(self.nologsleep)
				continue
			notify_msg=""
			found=0

			# start the timer
			self.log_time_start=time.time()

			lines=self.fd.readlines()
			if self.debug > 0:
				print >> sys.stderr, "DEBUG: readlines() returned %d lines" % len(lines)
			for line in lines:
				# if we have a partial line from last time, use it
				if len(save_line) > 0:
					if self.debug:
						print >> sys.stderr, "DEBUG: Reassembling Line: %s||+||%s" % (save_line, line)
					line=save_line+line
					save_line=""
				# make sure it's a complete line before continuing
				if line[-1:] == '\n':
					# check for lines to ignore before doing anything else
					try:
						if self.ignore.search(line):
							#print "Ignoring: %s" % line
							self.ignored_count+=1
							continue
							
					except Exception, e:
						print "Exception: %s" % e

					# handle plugins
					for p in self.plugins:
						try:
							p.process_line(line)
						except Exception, e:
							print >> sys.stderr, "Failed to call process_line on plugin %s (%s: %s)" % (p.__class__.__name__, Exception, e)

					try:
						self.requests+=1
						#print self.requests

						# we will also count lines that didn't match, for proper ratio
						for cmetric in self.metrics_count_list:
							m=self.regex[cmetric].search(line)
							if m != None:
								# to make this ganglia-safe, need to encode or otherwise
								# clean the second argument
								key="%s_%s" % (cmetric, self.metric_cleaner.sub("_", m.group(1)))
							else:
								key="%s_%s" % (cmetric, "NotSet")
							try:
								self.metric_counts[key]+=1
							except Exception, e:
								self.metric_counts[key]=1
								if self.debug:
									print >> sys.stderr, "DEBUG: Found new count metric: %s" % (key)
								self.new_metric_count+=1

						# this is just like processing_time, but without s/ms support
						for smetric in self.metrics_sum_list:
							m=self.regex[smetric].search(line)
							if m != None:
								value=float(m.group(1))
								try:
									self.metric_sums[smetric]+=value
								except Exception, e:
									self.metric_sums[smetric]=value
									if self.debug:
										print >> sys.stderr, "DEBUG: Found new sum metric: %s" % (smetric)
									self.new_metric_count+=1

						# search for distribution metrics
						for dmetric in self.metrics_dist_list:
							m=self.regex[dmetric].search(line)
							if m != None:
								value=int(m.group(1))
								bucket=value / self.metric_dist_bucketsize[dmetric]
								#print >> sys.stderr, "%d -> %d" % (value, bucket)
								if bucket > self.metric_dist_bucketcount[dmetric]-1:
									bucket=self.metric_dist_bucketcount[dmetric]-1
								try:
									self.metric_dists[dmetric][bucket]+=1
								except Exception, e:
									self.metric_dists[dmetric][bucket]=1

						# processing_time
						m=self.regex["processing_time"].search(line)
						if m != None:
							pt=float(m.group(1))
							if self.processing_time_units == "ms":
								pt = pt / 1000.0
							elif self.processing_time_units == "us":
								pt = pt / 1000.0 / 1000.0
							self.processing_time+=pt
							if pt > self.max_processing_time:
								self.max_processing_time=pt
							if pt > self.sla:
								self.pt_requests_exceeding_sla+=1
							self.pt_requests+=1

						if self.use_brand:
							# brand (how about pt/brand?)
							m=self.regex["brand"].search(line)
							if m != None:
								brand=m.group(1)
							else:
								brand="NULL_brand"

							self.logbrand(brand)
					except Exception, e:
						if self.debug > 0:
							print "Continuing after exception [3]: %s" % e
						continue
				else:
					# incomplete line: save
					save_line=line
					if self.debug:
						print >> sys.stderr, "DEBUG: Incomplete Line, saving: %s" % (save_line)

				self.prev_pos=self.curr_pos

			# add to the timer
			self.log_time+=time.time() - self.log_time_start


def handleSignal(signum, frame):
	print "\nLogWatcher killed"
	sys.exit(signum)
		

def procdaemonize (stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
	# Do first fork.
	try: 
		pid = os.fork() 
		if pid > 0:
			sys.exit(0) # Exit first parent.
	except OSError, e: 
		sys.stderr.write ("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
		sys.exit(1)
		
	# Decouple from parent environment.
	os.chdir("/") 
	os.umask(0) 
	os.setsid() 
	
	# Do second fork.
	try: 
		pid = os.fork() 
		if pid > 0:
			sys.exit(0) # Exit second parent.
	except OSError, e: 
		sys.stderr.write ("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror))
		sys.exit(1)
		
	# Now I am a daemon!
	
	# Redirect standard file descriptors.
	si = file(stdin, 'r')
	so = file(stdout, 'a+')
	se = file(stderr, 'a+', 0)
	os.dup2(si.fileno(), sys.stdin.fileno())
	os.dup2(so.fileno(), sys.stdout.fileno())
	os.dup2(se.fileno(), sys.stderr.fileno())


def main(argv):
	import getopt, sys
	try:
		opts, args = getopt.getopt(argv, "VDdg:Gvhpc:i:qbt", ["verbose", "debug", "daemonize", "graphite-server", "use-graphite", "version", "help", "pidfile=", "config=", "distinguisher=","quit","beginning","testconfig"])
	except:
		usage()

	pidfile=None
	configfile=None
	daemonize=0
	debug=0
	distinguisher=None
	quit=False
	beginning=False
	testconfig=False
	graphite_server=None
	use_graphite=False

	for opt, arg in opts:
		if opt in ("-h", "--help"):
			usage()
			sys.exit(0)
		if opt in ("-v", "--version"):
			print code_version
			sys.exit(0)
		if opt in ("-p", "--pidfile"):
			pidfile=arg
		if opt in ("-i", "--distinguisher"):
			distinguisher=arg
		if opt in ("-c", "--config"):
			configfile=arg
		if opt in ("-g", "--graphite-server"):
			graphite_server=arg
		if opt in ("-G", "--use-graphite"):
			use_graphite=True
		if opt in ("-d", "--daemonize"):
			daemonize=1
		if opt in ("-D", "--debug"):
			debug=1
		if opt in ("-V", "--verbose"):
			debug=2
		if opt in ("-q", "--quit"):
			quit=True
		if opt in ("-b", "--beginning"):
			beginning=True
		if opt in ("-t", "--testconfig"):
			testconfig=True

	lw=LogWatcher(pidfile, daemonize, configfile, distinguisher, debug, quit, beginning,testconfig,graphite_server, use_graphite)

def usage():
	print "usage: %s [-h] [-v] [-D] [-V] [-d] [ -c configfile ] [-i <distinguisher>] [-p <pidfile>] [-q] [-b] [-t]" % sys.argv[0]
	print "  -h --help                Print this message"
	print "  -v --version             Print the version"
	print "  -D --debug               Don't send metrics, just print them"
	print "  -g --graphite-server <s> Use graphite, with server <s>"
	print "  -G --use-graphite        Use graphite, find server in /etc/graphite.conf"
	print "  -V --verbose             Print gmetric commands as they're sent. Disables -D"
	print "  -d --daemonize           Run in the background"
	print "  -c --config <file>       Use the given configuration file"
	print "  -i --distinguisher <dis> Use the given string in the metric names"
	print "  -p --pidfile <file>      Store the PID in the given file"
	print "  -q --quit                Quit after sending metrics (useful with -D)"
	print "  -b --beginning           Read the log from the beginning (useful with -q)"
	print "  -t --testconfig          Read overrides from the \"test\" section of the configuration file"

if __name__ == "__main__":
	signal.signal(signal.SIGINT, handleSignal)
	main(sys.argv[1:])


