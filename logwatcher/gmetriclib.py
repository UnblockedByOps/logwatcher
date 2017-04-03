'''Gmetric implimentation for gmond.'''
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

import os
import re
import sys
import logging

# FIXME: This needs to be automated and become graphtie compatible
CODE_VERSION = "$Id: gmetriclib.py 173790 2012-06-29 23:00:44Z wil $"
LOG = logging.getLogger(__name__)

class Gmetric(object):
    '''Send metrics to gmond.'''

    def __init__(self,
                 metric_type,
                 name,
                 units,
                 maximum,
                 mcast=None,
                 debug=0):

        self.bin = "/usr/bin/gmetric"
        self.metric_type = metric_type
        self.name = name
        self.units = units
        self.maximum = maximum
        self.mcast = mcast
        self.mcast_if = ""
        self.debug = debug

        self.version = 2

        if mcast is None:
            self.get_mc_channel()

    def send(self, value, float_num=0):
        '''Send the metric.'''

        if float_num:
            value = "%.3f" % value
        cmd_v2 = "%s --type=%s --name=%s --value=%s --units=%s " \
                 "--tmax=%s --mcast_channel=%s %s" % (self.bin,
                                                      self.metric_type,
                                                      self.name,
                                                      value,
                                                      self.units,
                                                      self.maximum,
                                                      self.mcast,
                                                      self.mcast_if)
        cmd_v3 = "%s -c /etc/gmond.conf --type=%s --name=%s --value=%s " \
                 "--units=%s --tmax=%s" % (self.bin,
                                           self.metric_type,
                                           self.name,
                                           value,
                                           self.units,
                                           self.maximum)
        if self.version == 2:
            cmd = cmd_v2
        else:
            cmd = cmd_v3

        LOG.debug('COMMAND: {0}'.format(cmd))
        if self.debug == 1:
            ret = 0
        else:
            ret = os.system(cmd)

        if ret != 0:
            LOG.warn('There was an error running: {0} Switching to ganglia '
                     'version 3...'.format(cmd))
            if self.version == 2:
                self.version = 3
                LOG.debug('COMMAND: {0}'.format(cmd_v3))
                ret = os.system(cmd_v3)
                if ret != 0:
                    LOG.error('Version 3 fails as well!')
                else:
                    LOG.info('INFO version 3 works.')

    def get_mc_channel(self):
        '''Get multicast channel from config file.'''

        conf = "/etc/gmond.conf"
        if os.path.exists(conf):
            regex = re.compile("^mcast_channel\s+([\d.]+)")
            regex2 = re.compile("^mcast_if\s+(\w+)")
            try:
                conf_fd = open(conf, 'r')
                lines = conf_fd.readlines()
                for line in lines:
                    metric = regex.search(line)
                    if metric:
                        self.mcast = metric.group(1)
                    metric = regex2.search(line)
                    if metric:
                        self.mcast_if = "--mcast_if=%s" % metric.group(1)
                conf_fd.close()
                return 1
            except:
                LOG.error("Couldn't find mcast_channel in conf: {0}".format(conf))
                sys.exit(9)
        else:
            LOG.error('Conf does not exist: {0}'.format(conf))
            sys.exit(9)
