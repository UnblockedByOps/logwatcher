#!/usr/local/bin/python2.7
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
import sys
import signal
import argparse
import logging
from logwatcher.lw import LogWatcher
from logwatcher.common import read_graphite_conf

# FIXME: This needs to be automated and become graphtie compatible
CODE_VERSION = "$Id: logwatcher.py 233274 2014-06-23 23:20:52Z heitritterw $"
LOG = logging.getLogger(__name__)

# FIXME: Why does this need a second arg?
def handle_signal(signum, frame):
    '''Exit on a given signal number.'''

    print "\nLogWatcher killed"
    sys.exit(signum)

def parse_args():
    '''Parse all the command line arguments.'''

    help_desc = '''
    Watch logs for configured metrics and send them to graphite.

    >>> run.py -D -V -c /app/logwatcher/conf/logwatcher.ini -b -g graphite
    readlines()
    '''

    pap = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                  description=help_desc)

    pap.add_argument('-b',
                     '--beginning',
                     action='store_true',
                     help='Read the log from the beginning (useful with -q)')
    pap.add_argument('-c',
                     '--config',
                     help='Use the given configuration file',
                     default=None)
    pap.add_argument('-d',
                     '--daemonize',
                     action='store_true',
                     help="Run in the background.",
                     default=0)
    pap.add_argument('-D',
                     '--debug',
                     help="Don't send metrics, just print them.",
                     default=0)
    pap.add_argument('-G',
                     '--use-graphite',
                     action='store_true',
                     help='Use graphite, find server in /etc/graphite.conf.')
    pap.add_argument('-g',
                     '--graphite-server',
                     help='Use graphite, with server GRAPHITE_SERVER.',
                     default=None)
    pap.add_argument('-i',
                     '--distinguisher',
                     help='Use the given string in the metric names',
                     default=None)
    pap.add_argument('-l',
                     '--daemon-log',
                     help='Log file to write to in daemon mode. Useful for '
                     'debugging.',
                     default=None)
    pap.add_argument('-m',
                     '--metric-format',
                     help='Metric formatting to use. "ctg" or "trp".',
                     default="ctg")
    pap.add_argument('-o',
                     '--console-log',
                     help='Write a console log in daemon mode. Useful for '
                     'debugging.',
                     default='/dev/null')
    pap.add_argument('-P',
                     '--graphite-port',
                     help='The port number to use for graphite.',
                     default=2003)
    pap.add_argument('-p',
                     '--pidfile',
                     help='Store the PID in the given file',
                     default=None)
    pap.add_argument('-R',
                     '--prefix-root',
                     dest='prefix_root',
                     help='The prefix for all logwatcher metrics.',
                     default='LW_')
    pap.add_argument('-q',
                     '--quit',
                     action='store_true',
                     help="Quit after sending metrics (useful with -D).")
    pap.add_argument('-t',
                     '--testconfig',
                     action='store_true',
                     help='Read overrides from the [test] section of the '
                     'configuration file')
    pap.add_argument('-V',
                     '--verbose',
                     action='store_true',
                     help="Print gmetric commands as they're sent. Disables -D.")
    pap.add_argument('-v',
                     '--version',
                     action='store_true',
                     help="Print the version.")

    return pap.parse_args()

def main():
    '''Do all the things.'''

    # parse the args
    args = parse_args()

    # FIXME: convert to true/false or one option with levels.
    # debug = 0
    # for opt, arg in opts:
    #     if opt in ("-D", "--debug"):
    #         debug = 1
    #     if opt in ("-V", "--verbose"):
    #         debug = 2

    if args.debug == 0:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG

    root = logging.getLogger()
    root.setLevel(log_level)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(levelname)-8s- %(message)s')
    # Log to a file
    if args.daemon_log:
        logger = logging.getLogger('logwatcher')
        hdlr = logging.FileHandler(args.daemon_log)
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(log_level)
    console.setFormatter(formatter)
    root.addHandler(console)

    if args.version:
        LOG.info(CODE_VERSION)
        sys.exit(0)

    if args.use_graphite and not args.graphite_server:
        args.graphite_server = read_graphite_conf()
        if not args.graphite_server:
            LOG.error('Failed to set graphite server from config file. Using gmetric.')
        else:
            args.use_graphite = True

    if args.graphite_server:
        from logwatcher.graphitelib import Gmetric
        LOG.info('Using Gmetric Graphite.')
    else:
        from logwatcher.gmetriclib import Gmetric
        LOG.info('Using Gmetric Gmond.')

    LogWatcher(Gmetric,
               args.pidfile,
               args.daemonize,
               args.console_log,
               args.config,
               args.distinguisher,
               args.debug,
               args.quit,
               args.beginning,
               args.testconfig,
               args.graphite_server,
               args.graphite_port,
               args.use_graphite,
               args.prefix_root,
               args.metric_format)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_signal)
    main()
