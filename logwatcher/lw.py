#!/usr/local/bin/python2.7
'''Watch logs and report metrics to graphite or gmond.'''
#!/bin/env python
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
import time
import os
import re
import sys
import atexit
import ConfigParser
import signal
import logging
from logwatcher.common import send_to_graphite

# FIXME: This needs to be automated and become graphtie compatible
CODE_VERSION = "$Id: logwatcher.py 233274 2014-06-23 23:20:52Z heitritterw $"
LOG = logging.getLogger(__name__)

class LogWatcher(object):
    '''Main logwatcher class.'''

    def __init__(self,
                 Gmetric,
                 pidfile=None,
                 daemonize=False,
                 console_log=None,
                 configfile=None,
                 distinguisher=None,
                 debug=0,
                 quit_eof=False,
                 beginning=False,
                 testconfig=False,
                 graphite_server=None,
                 graphite_port=None,
                 use_graphite=False,
                 prefix_root=None,
                 metric_format=None):

        self.Gmetric = Gmetric
        self.log = ""
        self.fd = None

        self.graphite_server = graphite_server
        self.graphite_port = graphite_port
        if self.graphite_server:
            self.use_graphite = True
        else:
            self.use_graphite = use_graphite

        self.console_log = console_log
        self.metric_format = metric_format

        # initializing, will be populated later
        self.plugin_list = []
        self.plugins = []
        self.plugin_dir = None
        self.plugin_paths = ["/app/logwatcher/plugins", os.path.dirname(__file__)+"/plugins"]
        self.gmetric_brands = {}
        self.regex = {}
        self.gmetric = {}
        # metrics that count matching lines
        self.metric_counts = {}
        # metrics that sum values found
        self.metric_sums = {}
        # metrics that are calculated from other metrics
        self.metric_calcs = {}
        self.metric_calc_expr = {}

        # metrics that describe distributions
        self.metric_dists = {}
        self.metric_dist_bucketsize = {}
        self.metric_dist_bucketcount = {}

        self.ignore_pattern = ""
        self.ignore = None

        self.configfile = configfile

        self.debug = debug
        self.pidfile = pidfile
        self.distinguisher = distinguisher
        self.quit_eof = quit_eof
        self.beginning = beginning
        self.testconfig = testconfig

        self.log_time = 0
        self.log_time_start = 0
        self.notify_time = 0
        self.notify_time_start = 0

        self.read_config()
        signal.signal(signal.SIGHUP, self.reread_config)

        self.new_metric_count = 0 # counts new-found dynamic metrics
        self.total_metric_count = 0 # counts metrics sent

        self.prefix_root = prefix_root
        self.prefix = self.prefix_root
        if self.distinguisher:
            self.prefix = "%s%s_" % (self.prefix, self.distinguisher)

        self.daemonize = daemonize

        if self.get_pid() < 1:
            if self.daemonize:
                lw_daemonize(self.console_log,
                             self.console_log,
                             self.console_log)

        if self.lock_pid() == 0:
            LOG.error('Pidfile found')
            sys.exit(-1)

        self.log_count = 0 # how many different logs have we opened?
        self.curr_pos = 0
        self.prev_pos = 0
        self.last_time = time.time()

        self.brand_counts = {}

        self.gmetric["Q"] = self.Gmetric("float",
                                         "%sQueries" % self.prefix,
                                         "count",
                                         self.notify_schedule,
                                         self.graphite_server,
                                         self.graphite_port,
                                         self.metric_format,
                                         self.debug)
        self.gmetric["QPS"] = self.Gmetric("float",
                                           "%sQPS" % self.prefix,
                                           "qps",
                                           self.notify_schedule,
                                           self.graphite_server,
                                           self.graphite_port,
                                           self.metric_format,
                                           self.debug)
        self.gmetric["APT"] = self.Gmetric("float",
                                           "%sAvg_Processing_Time" % self.prefix,
                                           "seconds",
                                           self.notify_schedule,
                                           self.graphite_server,
                                           self.graphite_port,
                                           self.metric_format,
                                           self.debug)
        self.gmetric["MAX"] = self.Gmetric("float",
                                           "%sMax_Processing_Time" % self.prefix,
                                           "seconds",
                                           self.notify_schedule,
                                           self.graphite_server,
                                           self.graphite_port,
                                           self.metric_format,
                                           self.debug)
        self.gmetric["TPT"] = self.Gmetric("float",
                                           "%sTotal_Processing_Time" % self.prefix,
                                           "seconds",
                                           self.notify_schedule,
                                           self.graphite_server,
                                           self.graphite_port,
                                           self.metric_format,
                                           self.debug)
        self.gmetric["SLA"] = self.Gmetric("float",
                                           "%sexceeding_SLA" % self.prefix,
                                           "percent",
                                           self.notify_schedule,
                                           self.graphite_server,
                                           self.graphite_port,
                                           self.metric_format,
                                           self.debug)
        self.gmetric["SLA_ct"] = self.Gmetric("float",
                                              "%sexceeding_SLA_ct" % self.prefix,
                                              "percent",
                                              self.notify_schedule,
                                              self.graphite_server,
                                              self.graphite_port,
                                              self.metric_format,
                                              self.debug)
        self.gmetric["code_version"] = self.Gmetric("string",
                                                    "%sLW_Version" % self.prefix_root,
                                                    "string",
                                                    self.notify_schedule,
                                                    self.graphite_server,
                                                    self.graphite_port,
                                                    self.metric_format,
                                                    self.debug)
        self.gmetric["ignore"] = self.Gmetric("float",
                                              "%signored" % self.prefix,
                                              "count",
                                              self.notify_schedule,
                                              self.graphite_server,
                                              self.graphite_port,
                                              self.metric_format,
                                              self.debug)
        self.gmetric["NOTIFY_TIME"] = self.Gmetric("float",
                                                   "%s%s" % (self.prefix_root, "LW_NotifyTime"),
                                                   "seconds", self.notify_schedule,
                                                   self.graphite_server,
                                                   self.graphite_port,
                                                   self.metric_format,
                                                   self.debug)
        self.gmetric["LOG_TIME"] = self.Gmetric("float",
                                                "%s%s" % (self.prefix_root, "LW_LogTime"),
                                                "seconds",
                                                self.notify_schedule,
                                                self.graphite_server,
                                                self.graphite_port,
                                                self.metric_format,
                                                self.debug)
        self.gmetric["NEW_METRICS"] = self.Gmetric("float",
                                                   "%s%s" % (self.prefix_root, "LW_NewMetrics"),
                                                   "float",
                                                   self.notify_schedule,
                                                   self.graphite_server,
                                                   self.graphite_port,
                                                   self.metric_format,
                                                   self.debug)
        self.gmetric["TOTAL_METRICS"] = self.Gmetric("float",
                                                     "%s%s" % (self.prefix_root, "LW_TotalMetrics"),
                                                     "float",
                                                     self.notify_schedule,
                                                     self.graphite_server,
                                                     self.graphite_port,
                                                     self.metric_format,
                                                     self.debug)

        # use this for sub-hourly and other odd log rotation
        self.curr_inode = None

        self.prime_metrics()

        self.initialize_counters()
        self.watch()

    # FIXME: Consolidate config read.
    def read_test_config(self):
        '''Read the test section of the conf.'''

        sec = "test"

        if self.configfile == None:
            return 0
        try:
            parse = ConfigParser.ConfigParser()
            parse.read(self.configfile)
            self.logformat = parse.get(sec, "log_name_format")
        except:
            pass

        try:
            self.notify_schedule = int(parse.get(sec, "notify_schedule"))
        except:
            pass

    def reread_config(self, signum, frame):
        '''Signal hadler for re-reading the config file.'''

        self.read_config()

    # FIXME: Consolidate config read.
    def read_config(self):
        '''Read in logwatcher config files.'''

        LOG.debug('readconfig() called')
        sec = "logwatcher"

        if self.configfile is None:
            return 0
        try:
            cp = ConfigParser.ConfigParser()
            cp.read(self.configfile)
            self.logformat = cp.get(sec, "log_name_format")

            if not self.graphite_server:
                try:
                    self.use_graphite = cp.getboolean(sec, "use_graphite")
                except:
                    pass

            # "except -> pass" for those that come in via commandline
            if self.pidfile is None:
                try:
                    self.pidfile = cp.get(sec, "pidfile")
                except:
                    pass

            if not self.plugin_dir:
                try:
                    self.plugin_list = cp.get(sec, "plugin_dir")
                except:
                    pass

            if self.plugin_dir:
                if os.path.exists(self.plugin_dir):
                    sys.path.append(self.plugin_dir)
                else:
                    LOG.error('ERROR: {0} does not exist'.format(self.plugin_dir))
            else:
                for ppath in self.plugin_paths:
                    if os.path.exists(ppath):
                        sys.path.append(ppath)
                        break

            if not self.plugin_list:
                try:
                    self.plugin_list = cp.get(sec, "plugins").split()
                except:
                    pass

            LOG.info('Loading plugins: {0}'.format(self.plugin_list))
            try:
                for plugin in self.plugin_list:
                    print >> sys.stderr, "Loading plugin: %s" % (plugin)
                    # import the module
                    mod = __import__(plugin)
                    # name the class so we can call it
                    cls = getattr(mod, plugin)
                    # create an instance of the class
                    self.plugins.append(cls(self.debug, self.get_plugin_conf(plugin)))
            except Exception as ex:
                LOG.error('Failed to load plugin: {0} ({1})'.format(Exception, ex))
                sys.exit(4) # should it be this serious?

            import string
            self.sla = float(cp.get(sec, "sla_ms"))/1000.0 # self.sla is seconds

            try:
                self.nologsleep = int(cp.get(sec, "nologsleep"))
            except:
                self.nologsleep = 10

            try:
                self.notify_schedule = int(cp.get(sec, "notify_schedule"))
            except:
                self.notify_schedule = 60

            try:
                self.debug = int(cp.get(sec, "debug"))
            except:
                pass

            LOG.debug('{0}'.format(self.notify_schedule))
            self.regex["processing_time"] = re.compile(cp.get(sec, "processing_time_regex"))
            self.processing_time_units = cp.get(sec, "processing_time_units")

            self.use_brand = 0
            try:
                use_brand = int(cp.get(sec, "use_brand"))
                if use_brand == 1:
                    self.use_brand = 1
            except:
                pass

            if self.use_brand == 1:
                self.regex["brand"] = re.compile(cp.get(sec, "brand_regex"))

            if self.distinguisher is None:
                try:
                    self.distinguisher = cp.get(sec, "distinguisher")
                except:
                    pass

            # read in the metrics to prime
            try:
                self.metrics_prime_list = cp.get(sec, "metrics_prime").split(" ")
            except:
                self.metrics_prime_list = ()

            # read in the Count metrics, and optionally, the ratio metrics
            self.metrics_count_list = cp.get(sec, "metrics_count").split(" ")
            try:
                self.metrics_ratio_list = cp.get(sec, "metrics_ratio").split(" ")
            except:
                self.metrics_ratio_list = ()
            for metric in self.metrics_count_list:
                self.regex[metric] = re.compile(cp.get(sec, "metric_%s_regex" % metric))

            # read in the Sum metrics; these can be ratio metrics as well!
            try:
                self.metrics_sum_list = cp.get(sec, "metrics_sum").split(" ")
                to_remove= []
                for metric in self.metrics_sum_list:
                    try:
                        self.regex[metric] = re.compile(cp.get(sec, "metric_%s_regex" % metric))
                    except:
                        LOG.error('ERROR: Failed to find metric_{0}_regex!'.format(metric))
                        # remove it after we leave the loop
                        to_remove.append(metric)
                for rem in to_remove:
                    self.metrics_sum_list.remove(rem)
            except Exception as ex:
                LOG.error('Error reading metrics_sum: {0}'.format(ex))
                self.metrics_sum_list = ()

            # read in the calc metrics
            try:
                self.metrics_calc_list = cp.get(sec, "metrics_calc").split(" ")
                for metric in self.metrics_calc_list:
                    try:
                        self.metric_calc_expr[metric] = cp.get(sec, "metric_%s_expression" % metric)
                    except:
                        LOG.error('Failed to find metric_{0}_regex!'.format(metric))
                        self.metrics_calc_list.remove(metric)
            except:
                self.metrics_calc_list = ()

            # read in the distribution metrics
            try:
                self.metrics_dist_list = cp.get(sec, "metrics_dist").split(" ")
                for metric in self.metrics_dist_list:
                    try:
                        self.metric_dist_bucketsize[metric] = int(cp.get(sec, "metric_%s_bucket_size" % metric))
                        self.metric_dist_bucketcount[metric] = int(cp.get(sec, "metric_%s_bucket_count" % metric))
                        self.regex[metric] = re.compile(cp.get(sec, "metric_%s_regex" % metric))
                    except Exception as ex:
                        LOG.error('ERROR: Failed to set up metric_{0}_regex!  ({1})'.format(metric,
                                                                                            ex))
                        self.metrics_dist_list.remove(metric)
            except:
                self.metrics_dist_list = ()

            # Get the ignore pattern. We'll completely ignore (but count) any matching lines.
            try:
                self.ignore_pattern = cp.get(sec, "ignore_pattern")
            except:
                self.ignore_pattern = "^$" # safe to ignore
            self.ignore = re.compile(self.ignore_pattern)

            # this will be used to cleanse "found" metric names
            try:
                self.metric_cleaner = re.compile(cp.get(sec, "metric_cleaner"))
            except:
                self.metric_cleaner = re.compile("[/.:;\"\' $=]")

            # FIXME: need some error handling for ratios that don't exist

        except Exception as ex:
            req_options = [
                'log_name_format',
                'sla_ms',
                'processing_time_regex',
                'use_brand',
                'brand_regex',
                'metrics_count',
                '    metric_<metric_name>_regex for any metric listed in metrics_count',
            ]
            LOG.error('failed to parse config file: {0}'.format(self.configfile))
            LOG.error('The following options are required:')
            for req in req_options:
                LOG.error(' {0}'.format(req))
            LOG.error('Root error: {0}'.format(ex))
            sys.exit(1)
        if self.testconfig:
            self.read_test_config()

    def get_plugin_conf(self, plugin):
        '''Read plugin config from conf file.'''

        LOG.debug('get_plugin_conf({0}) called.'.format(plugin))

        if self.configfile is None:
            return 0
        try:
            parse = ConfigParser.ConfigParser()
            parse.read(self.configfile)
            return dict(parse.items(plugin))
        except:
            return {}

    def lock_pid(self):
        '''Write the PID file.'''

        pid = self.get_pid()
        if pid == -1: # not using pidfile
            return 1
        elif pid == 0: # no pidfile
            atexit.register(self.remove_pid)
            myf = open(self.pidfile, "w")
            myf.write("%d" % os.getpid())
            myf.close()
            return 1
        else:
            LOG.info('PID is {0}'.format(pid))
            return 0

        if os.path.exists(self.pidfile):
            return 0

    def remove_pid(self):
        '''Remove the PID file.'''

        try:
            os.unlink(self.pidfile)
        except:
            LOG.warn('unable to unlink pidfile! {0}'.format(self.pidfile))

    def get_pid(self):
        '''Return the contents of the PID file if it exists.'''

        if not self.pidfile:
            return -1
        if os.path.exists(self.pidfile):
            myf = open(self.pidfile)
            myp = myf.read()
            myf.close()
            return int(myp)
        else:
            return 0

    def prime_metrics(self):
        '''Unknown.'''

        for pair in self.metrics_prime_list:
            try:
                pmetric, val = pair.split(":")
                met = self.Gmetric("float",
                                   "%s%s" % (self.prefix, pmetric),
                                   "prime",
                                   self.notify_schedule,
                                   self.graphite_server,
                                   self.graphite_port,
                                   self.metric_format,
                                   self.debug)
                met.send(float(val), 1)
                self.total_metric_count += 1
            except Exception as ex:
                LOG.warn('Failed to send prime metric {0} ({1})'.format(pair, ex))

    def notifybrand(self, brand, seconds):
        '''Not sure what this does.'''

        try:
            if not self.gmetric_brands.has_key(brand):
                self.gmetric_brands[brand] = self.Gmetric("float",
                                                          "%sQPS_%s" % (self.prefix, brand),
                                                          "qps",
                                                          self.notify_schedule,
                                                          self.graphite_server,
                                                          self.graphite_port,
                                                          self.metric_format,
                                                          self.debug)
            self.gmetric_brands[brand].send(float(self.brand_counts[brand]/seconds), 1)
            self.total_metric_count += 1
        except Exception as ex:
            LOG.warn("Couldn't notify for brand {0} ({1})".format(brand, ex))

    def notify(self, seconds):
        '''Send metrics.'''

        self.notify_time_start = time.time()
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
            qps = float(self.requests/seconds)
        else:
            qps = 0.0
        self.gmetric["Q"].send(self.requests, 1)
        self.gmetric["QPS"].send(qps, 1)
        #print self.processing_time
        self.total_metric_count += 7

        LOG.debug('covered {0}, requests {1}'.format(self.covered,
                                                     self.requests))
        if self.requests > 0:
            coverage_per_query = self.covered*100.0/self.requests
        else:
            coverage_per_query = 0.0
        #print "served %d, possible %d" % (self.inventory_served,self.inventory_possible)
        if self.inventory_possible > 0:
            coverage_per_ad_requested = self.inventory_served*100.0/self.inventory_possible
        else:
            coverage_per_ad_requested = 0.0

        #self.gmetric_cpq.send(coverage_per_query, 1)
        #self.gmetric_cpar.send(coverage_per_ad_requested, 1)

        if not self.use_graphite:
            self.gmetric["code_version"].send("\"%s\"" % CODE_VERSION, 0)
        self.gmetric["ignore"].send(self.ignored_count, 1)
        self.total_metric_count += 2

        for brand in self.brand_counts.keys():
            self.notifybrand(brand, seconds)

        for rmetric in self.metrics_ratio_list:
            tot = 0
            regex = re.compile("^%s" % rmetric)
            for smetric in self.metric_sums.keys():
                rmetric_name = "%s_ratio" % smetric
                if re.match(regex, smetric):
                    if self.requests != 0:
                        # we don't want to multiply by 100 for sum ratios
                        perc = float(self.metric_sums[smetric])/float(self.requests)
                    else:
                        perc = 0.0
                    try:
                        self.gmetric[rmetric_name].send(perc, 1)
                    except: #sketchy
                        self.gmetric[rmetric_name] = self.Gmetric("float",
                                                                  "%s%s" %
                                                                  (self.prefix, rmetric_name),
                                                                  "percent",
                                                                  self.notify_schedule,
                                                                  self.graphite_server,
                                                                  self.graphite_port,
                                                                  self.metric_format,
                                                                  self.debug)
                        self.gmetric[rmetric_name].send(perc, 1)
                    self.total_metric_count += 1

            for cmetric in self.metric_counts.keys():
                if re.match(regex, cmetric):
                    tot = tot+self.metric_counts[cmetric]
                    #print "TOTAL %d" % tot
            for cmetric in self.metric_counts.keys():
                rmetric_name = "%s_ratio" % cmetric
                if re.match(regex, cmetric):
                    if tot != 0:
                        perc = float(self.metric_counts[cmetric])/float(tot) * 100
                    else:
                        perc = 0.0
                    #print "%s %s %.2f" % (self.metric_counts[cmetric], cmetric, perc)
                    try:
                        self.gmetric[rmetric_name].send(perc, 1)
                    except: #sketchy
                        self.gmetric[rmetric_name] = self.Gmetric("float",
                                                                  "%s%s" %
                                                                  (self.prefix, rmetric_name),
                                                                  "percent",
                                                                  self.notify_schedule,
                                                                  self.graphite_server,
                                                                  self.graphite_port,
                                                                  self.metric_format,
                                                                  self.debug)
                        self.gmetric[rmetric_name].send(perc, 1)
                    self.total_metric_count += 1

        # send smetrics
        for smetric in self.metric_sums.keys():
            LOG.debug('sending {:.2f}'.format(self.metric_sums[smetric]))
            try:
                self.gmetric[smetric].send(self.metric_sums[smetric], 1)
            except: #sketchy
                self.gmetric[smetric] = self.Gmetric("float",
                                                     "%s%s" % (self.prefix, smetric),
                                                     "sum",
                                                     self.notify_schedule,
                                                     self.graphite_server,
                                                     self.graphite_port,
                                                     self.metric_format,
                                                     self.debug)
                self.gmetric[smetric].send(self.metric_sums[smetric], 1)
            self.total_metric_count += 1

        # send cmetrics
        for cmetric in self.metric_counts.keys():
            LOG.debug('sending {:.2f}'.format(self.metric_counts[cmetric]))
            try:
                self.gmetric[cmetric].send(self.metric_counts[cmetric], 1)
            except: #sketchy
                self.gmetric[cmetric] = self.Gmetric("float",
                                                     "%s%s" % (self.prefix, cmetric),
                                                     "count",
                                                     self.notify_schedule,
                                                     self.graphite_server,
                                                     self.graphite_port,
                                                     self.metric_format,
                                                     self.debug)
                self.gmetric[cmetric].send(self.metric_counts[cmetric], 1)
            self.total_metric_count += 1

        # send emetrics/calcs
        for emetric in self.metric_calcs.keys():
            try:
                cvalue = self.calculate(self.metric_calc_expr[emetric])
            except Exception, e:
                print Exception, e
                cvalue = 0
            LOG.debug('emetric sending {:.2f} for {}'.format(cvalue, emetric))

            try:
                self.gmetric[emetric].send(cvalue, 1)
            except Exception, e: #sketchy, create then send instead of pre-initializing
                self.gmetric[emetric] = self.Gmetric("float",
                                                     "%s%s" % (self.prefix, emetric),
                                                     "expression",
                                                     self.notify_schedule,
                                                     self.graphite_server,
                                                     self.graphite_port,
                                                     self.metric_format,
                                                     self.debug)
                self.gmetric[emetric].send(cvalue, 1)
            self.total_metric_count += 1

        # send dmetrics
        for dmetric in self.metric_dists.keys():
            regex = re.compile("^%s" % rmetric)

            # Let's do the ratio metrics in-line here
            do_ratio = False
            if re.match(regex, dmetric):
                do_ratio = True

            last = 0
            for bucket in range(self.metric_dist_bucketcount[dmetric]):
                current = last+self.metric_dist_bucketsize[dmetric]
                # first bucket
                if last == 0:
                    dmetric_b = "%s_%d-%d" % (dmetric, 0, current-1)
                # last bucket
                elif bucket == self.metric_dist_bucketcount[dmetric]-1:
                    dmetric_b = "%s_%d-%s" % (dmetric, last, "inf")
                # other buckets
                else:
                    dmetric_b = "%s_%d-%d" % (dmetric, last, current-1)
                last = current
                #print dmetric_b,self.metric_dists[dmetric][bucket]
                LOG.debug('Sending {:.2f}'.format(self.metric_counts[dmetric_b][bucket]))
                try:
                    self.gmetric[dmetric_b].send(self.metric_counts[dmetric_b], 1)
                except: #sketchy
                    self.gmetric[dmetric_b] = self.Gmetric("float",
                                                           "%s%s" % (self.prefix, dmetric_b),
                                                           "count",
                                                           self.notify_schedule,
                                                           self.graphite_server,
                                                           self.graphite_port,
                                                           self.metric_format,
                                                           self.debug)
                    self.gmetric[dmetric_b].send(self.metric_dists[dmetric][bucket], 1)
                self.total_metric_count += 1

                if self.requests != 0:
                    # we don't want to multiply by 100 for sum ratios
                    perc = float(self.metric_dists[dmetric][bucket])/float(self.requests) * 100
                    #perc=float(self.metric_counts[cmetric])/float(tot) * 100 # do we need to count matches (tot)?
                else:
                    perc = 0.0
                try:
                    self.gmetric[dmetric_b+"_ratio"].send(perc, 1)
                except: #sketchy
                    self.gmetric[dmetric_b+"_ratio"] = self.Gmetric("float",
                                                                    "%s%s_ratio" % (self.prefix,
                                                                                    dmetric_b),
                                                                    "percent",
                                                                    self.notify_schedule,
                                                                    self.graphite_server,
                                                                    self.graphite_port,
                                                                    self.metric_format,
                                                                    self.debug)
                    self.gmetric[dmetric_b+"_ratio"].send(perc, 1)
                self.total_metric_count += 1

        # send plugin metrics
        for plugin in self.plugins:
            try:
                pmetrics = plugin.get_metrics()
            except Exception as ex:
                LOG.warn('{0}.get_metrics() failed. ({1})'.format(plugin.__class__.__name__, ex))
                continue
            for pmetric in pmetrics.keys():
                pmn = "plugins.%s.%s" % (plugin.__class__.__name__, pmetric)
                try:
                    self.gmetric[pmn].send(pmetrics[pmetric], 1)
                except: #sketchy
                    self.gmetric[pmn] = self.Gmetric("float",
                                                     "%s%s" % (self.prefix, pmn),
                                                     "count",
                                                     self.notify_schedule,
                                                     self.graphite_server,
                                                     self.graphite_port,
                                                     self.metric_format,
                                                     self.debug)
                    self.gmetric[pmn].send(pmetrics[pmetric], 1)
                self.total_metric_count += 1

        self.gmetric["LOG_TIME"].send(self.log_time, 1)
        self.gmetric["NEW_METRICS"].send(self.new_metric_count, 1)
        self.total_metric_count += 3 # includes the next line
        self.gmetric["TOTAL_METRICS"].send(self.total_metric_count, 1)

		# Batch all the graphtie metrics and send them.
        if self.graphite_server:
            metric_buffer = ""
            for metric in self.gmetric:
                metric_buffer += "%s\n" % self.gmetric[metric].pop()
            for metric in self.gmetric_brands:
                metric_buffer += "%s\n" % self.gmetric_brands[metric].pop()

            send_to_graphite(metric_buffer, self.graphite_server, self.graphite_port)

        # after sending batch, stop the timer
        self.notify_time = time.time() - self.notify_time_start

        # ...the one place where we changed the call for graphite
        if self.graphite_server:
            self.gmetric["NOTIFY_TIME"].send(self.notify_time, autocommit=True)
        else:
            self.gmetric["NOTIFY_TIME"].send(self.notify_time, 1)

        if self.quit_eof:
            LOG.info("Metrics complete.")
            sys.exit(0)

        self.initialize_counters()

    def initialize_counters(self):
        # processing_time
        self.processing_time = 0
        self.max_processing_time = 0
        self.requests = 0
        self.pt_requests = 0
        self.pt_requests_exceeding_sla = 0
        for brand in self.brand_counts.keys():
            self.brand_counts[brand] = 0

        for cmetric in self.metric_counts.keys():
            self.metric_counts[cmetric] = 0
        for smetric in self.metric_sums.keys():
            self.metric_sums[smetric] = 0
        # this one is different, since the dict isn't created while reading the log
        for emetric in self.metrics_calc_list:
            self.metric_calcs[emetric] = 0

        for dmetric in self.metrics_dist_list:
            self.metric_dists[dmetric] = {}
            for bucket in range(self.metric_dist_bucketcount[dmetric]):
                self.metric_dists[dmetric][bucket] = 0

        # coverage
        self.inventory_possible = 0
        self.covered = 0
        self.inventory_served = 0
        self.ignored_count = 0

        self.notify_time = 0
        self.log_time = 0
        self.new_metric_count = 0
        self.total_metric_count = 0

    def logbrand(self, brand, pt=None, coverate=None):
        if self.brand_counts.has_key(brand):
            self.brand_counts[brand] += 1
        else:
            self.brand_counts[brand] = 1
            if self.debug:
                LOG.debug('Found new publisher: {0}'.format(brand))
            self.new_metric_count += 1

    def openlog(self):
        try:
            if self.fd:
                self.fd.close()
                if self.debug:
                    LOG.debug('Closing existing logfile')
        except:
            LOG.warn('Existing logfile close() failed')
        try:
            self.fd = open(self.log, 'r')
            if self.debug:
                LOG.debug('Opening logfile {0}'.format(self.log))
                LOG.debug('Log count = {0}'.format(self.log_count))
            # go to end of the log unless we override (w/beginning) or ARE in the first log
            if ((not self.beginning) and (self.log_count == 0)):
                self.fd.seek(0, 2)
                if self.debug:
                    LOG.debug('GOING TO THE END')
            self.log_count += 1
            self.curr_pos = self.prev_pos = self.fd.tell()
            self.curr_inode = os.stat(self.log)[1]
            if self.debug:
                LOG.debug('Current position is {0}'.format(self.curr_pos))
        except Exception as ex:
            LOG.error('Error in openlog(): {0}'.format(str(ex)))
            sys.exit(9)

    def setlogname(self):
        '''Check for new log files.'''

        nowfile = time.strftime(self.logformat)
        if nowfile == self.log:
            #print "existing log"
            # should return 1 if log filename changed OR if inode changed!
            try:
                filename_inode = os.stat(nowfile)[1]
                if self.curr_inode != filename_inode:
                    return 1
            except Exception, e:
                # file probably renamed, but no new one yet
                pass
            return 0
        if os.path.exists(nowfile):
            if self.debug:
                LOG.debug('FOUND A NEW LOGFILE, we should switch (after finishing)')
            self.log = nowfile
            return 1
        return 0

    def parse_expression(self, expression):
        '''This will replace variables with values in an expression unknown items
        will be replaced with '_unknown_', forcing an exception at calculate()
        time'''

        nexpression = ""
        try:
            for bit in expression.split(" "):
                try:
                    value = float(bit)
                except (ValueError, TypeError):
                    if bit[:2] == "s/":
                        try:
                            value = float(self.metric_sums[bit[2:]])
                        except:
                            LOG.warn('In parse_expression() value for {0} not found'.format(bit))
                            value = float(0)
                    elif bit[:2] == "c/":
                        try:
                            value = float(self.metric_counts[bit[2:]])
                        except:
                            LOG.warn('In parse_expression() value for {0} not found'.format(bit))
                            LOG.warn(self.metric_counts.keys())
                            value = float(0)
                    # allow any object property to be used
                    elif bit[:2] == "i/":
                        try:
                            value = float(getattr(self, bit[2:]))
                        except:
                            LOG.warn('in parse_expression() value for {0} not found'.format(bit))
                            value = float(0)
                    elif bit in ('/', '+', '-', '*', '(', ')'):
                        value = bit
                    else:
                        value = "_unknown_"
                nexpression = "%s %s" % (nexpression, value)
        except Exception as ex:
            LOG.warn('Exception in parse_expression(): {0} ({1})'.format(Exception, ex))
            nexpression = "-1"
        return nexpression

    def calculate(self, expression):
        '''Evaluate a parsed user-configured expression.'''
        try:
            LOG.debug('calculate({0})'.format(self.parse_expression(expression)))
            value = eval(self.parse_expression(expression))
        except ZeroDivisionError as ex:
            LOG.warn('Division by zero in calculate({0})'.format(expression))
            value = 0
        except Exception as ex:
            value = -1
            LOG.error("Exception in calculate(): {0} (expression: '{1}')".format(ex, expression))
        return value

    def watch(self):
        '''Watch the log file for new lines.'''

        # save_line is a buffer for saving a partial line at the end of a read
        save_line = ""
        finish_counter = 0 # make sure we finished the previous file
        finish_tries = 3   # make sure we finished the previous file
        line = None
        while 1:
            now = time.time()
            if self.last_time+self.notify_schedule <= now:
                self.notify(now-self.last_time)
                self.last_time = now
            time.sleep(1)

            if self.setlogname() == 1:
                # we'll switch to the new log after trying the last log finish_tries times
                finish_counter += 1
                if self.debug:
                    LOG.debug('Last line was {0} (try {1})'.format(line, finish_counter))
                if self.fd is None or finish_counter >= finish_tries:
                    self.openlog()
                    finish_counter = 0
            elif self.fd is None:
                LOG.error('ERROR: logfile {0} not opened, sleeping {1}s'.format(self.log,
                                                                                self.nologsleep))
                time.sleep(self.nologsleep)
                continue
            notify_msg = ""
            found = 0

            # start the timer
            self.log_time_start = time.time()

            lines = self.fd.readlines()
            LOG.debug('readlines() returned {0} lines'.format(len(lines)))
            for line in lines:
                # if we have a partial line from last time, use it
                if len(save_line) > 0:
                    LOG.debug('Reassembling Line: {0}||+||{1}'.format(save_line,
                                                                       line))
                    line = save_line+line
                    save_line = ""
                # make sure it's a complete line before continuing
                if line[-1:] == '\n':
                    # check for lines to ignore before doing anything else
                    try:
                        if self.ignore.search(line):
                            #print "Ignoring: %s" % line
                            self.ignored_count += 1
                            continue

                    except Exception as ex:
                        LOG.warn('Exception: {0}'.format(ex))

                    # handle plugins
                    for plug in self.plugins:
                        try:
                            plug.process_line(line)
                        except Exception as ex:
                            LOG.warn('Failed to call process_line on plugin {0} ({1}: '
                                     '{2})'.format(plug.__class__.__name__, Exception, ex))

                    try:
                        self.requests += 1
                        #print self.requests

                        # we will also count lines that didn't match, for proper ratio
                        for cmetric in self.metrics_count_list:
                            m = self.regex[cmetric].search(line)
                            if m != None:
                                # to make this ganglia-safe, need to encode or otherwise
                                # clean the second argument
                                key = "%s_%s" % (cmetric, self.metric_cleaner.sub("_", m.group(1)))
                            else:
                                key = "%s_%s" % (cmetric, "NotSet")
                            try:
                                self.metric_counts[key] += 1
                            except Exception as ex:
                                self.metric_counts[key] = 1
                                LOG.debug('Found new count metric: {0}'.format(key))
                                self.new_metric_count += 1

                        # this is just like processing_time, but without s/ms support
                        for smetric in self.metrics_sum_list:
                            m = self.regex[smetric].search(line)
                            if m != None:
                                value = float(m.group(1))
                                try:
                                    self.metric_sums[smetric] += value
                                except Exception as ex:
                                    self.metric_sums[smetric] = value
                                    LOG.debug('Found new sum metric: {0}'.format(smetric))
                                    self.new_metric_count += 1

                        # search for distribution metrics
                        for dmetric in self.metrics_dist_list:
                            m = self.regex[dmetric].search(line)
                            if m != None:
                                value = int(m.group(1))
                                bucket = value / self.metric_dist_bucketsize[dmetric]
                                #print >> sys.stderr, "%d -> %d" % (value, bucket)
                                if bucket > self.metric_dist_bucketcount[dmetric]-1:
                                    bucket = self.metric_dist_bucketcount[dmetric]-1
                                try:
                                    self.metric_dists[dmetric][bucket] += 1
                                except Exception as ex:
                                    self.metric_dists[dmetric][bucket] = 1

                        # processing_time
                        m = self.regex["processing_time"].search(line)
                        if m != None:
                            pt = float(m.group(1))
                            if self.processing_time_units == "ms":
                                pt = pt / 1000.0
                            elif self.processing_time_units == "us":
                                pt = pt / 1000.0 / 1000.0
                            self.processing_time += pt
                            if pt > self.max_processing_time:
                                self.max_processing_time = pt
                            if pt > self.sla:
                                self.pt_requests_exceeding_sla += 1
                            self.pt_requests += 1

                        if self.use_brand:
                            # brand (how about pt/brand?)
                            m = self.regex["brand"].search(line)
                            if m != None:
                                brand = m.group(1)
                            else:
                                brand = "NULL_brand"

                            self.logbrand(brand)
                    except Exception as ex:
                        LOG.debug('Continuing after exception [3]: {0}'.format(ex))
                        continue
                else:
                    # incomplete line: save
                    save_line = line
                    LOG.debug('Incomplete Line, saving: {0}'.format(save_line))

                self.prev_pos = self.curr_pos

            # add to the timer
            self.log_time += time.time() - self.log_time_start


def lw_daemonize(stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    '''Daemonize this thing.'''

    LOG.info('Setting up daemon...')
    # Do first fork.
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0) # Exit first parent.
    except OSError as ex:
        LOG.error('fork #1 failed: ({0}) {1}'.format(ex.errno, ex.strerror))
        sys.exit(1)

    # Decouple from parent environment.
    os.chdir('/')
    os.umask(0)
    os.setsid()

    # Do second fork.
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0) # Exit second parent.
    except OSError as ex:
        LOG.error('fork #2 failed: ({0}) {1}'.format(ex.errno, ex.strerror))
        sys.exit(1)

    # Now I am a daemon!

    # Redirect standard file descriptors.
    sti = file(stdin, 'r')
    sto = file(stdout, 'a+')
    ste = file(stderr, 'a+', 0)
    os.dup2(sti.fileno(), sys.stdin.fileno())
    os.dup2(sto.fileno(), sys.stdout.fileno())
    os.dup2(ste.fileno(), sys.stderr.fileno())
    LOG.info('Logwatcher running in daemon mode...')
