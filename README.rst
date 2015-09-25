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

Description
===========

LogWatcher is a daemon that gathers metrics from the access logs of web applications, and provides them to Ganglia in near-realtime. Metrics are named with the prefix "LW_" for easy identification. An instance of LogWatcher is required for each access log that is to be watched. The log file can be either statically named or pre-rotated with timestamp in the filename. Metrics are typically collected/averaged by minute, but this is configurable.
