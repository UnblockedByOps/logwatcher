#!/bin/env python
'''Watch all the logs.'''
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
import sys
import argparse
import signal
from logwatcher.lw import LogWatcher

CODE_VERSION = "$Id: logwatcher.py 233274 2014-06-23 23:20:52Z heitritterw $"

# FIXME: Why does this need a second arg?
def handle_signal(signum, garbage):
    '''Exit on a given signal number.'''

    print "\nLogWatcher killed"
    sys.exit(signum)

def parse_args():
    '''Parse all the command line arguments.'''

    help_desc = '''
    Watch logs and send them to graphite.

    >>> lw_daemon.py -D -V -c /app/logwatcher/conf/logwatcher.ini -b -g graphite
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
                     dest='use_graphite',
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
    pap.add_argument('-p',
                     '--pidfile',
                     help='Store the PID in the given file',
                     default=None)
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

    # FIXME: Can these be converted to true/false?
    # debug = 0
    # for opt, arg in opts:
    #     if opt in ("-D", "--debug"):
    #         debug = 1
    #     if opt in ("-V", "--verbose"):
    #         debug = 2

    if args.version:
        print CODE_VERSION
        sys.exit(0)

    LogWatcher(args.pidfile,
               args.daemonize,
               args.config,
               args.distinguisher,
               args.debug,
               args.quit,
               args.beginning,
               args.testconfig,
               args.graphite_server,
               args.use_graphite)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_signal)
    main()
