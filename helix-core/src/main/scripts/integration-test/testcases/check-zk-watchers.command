#/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# colorful echo
red='\e[00;31m'
green='\e[00;32m'
function cecho
{
  message="$1"
  if [ -n "$message" ]; then
    color="$2"
    if [ -z "$color" ]; then
      echo "$message"
    else
      echo -e "$color$message\e[00m"
    fi
  fi
}

echo There are $# arguments to $0: $*
if [ "$#" -eq 2 ]; then
  zk_host=$1
  zk_port=$2
else
  echo "Usage: ./check-zk-wathcers zk_host zk_port"
  exit
fi

cons_file="/tmp/cons.$zk_host"
wchp_file="/tmp/wchp.$zk_host"
echo cons | nc $zk_host $zk_port > $cons_file
echo wchp | nc $zk_host $zk_port > $wchp_file

count=0
total_watch_count=0
while read line; do
  let count++
#  echo "$count: $line"
  session_id=`echo $line | awk 'BEGIN {FS="[(,)]"};{print $5}'`
#  echo "$session_id"
  if [ ! -z "$session_id" ]; then
    session_id=`echo $session_id | awk 'BEGIN {FS="[=]"};{print $2}'`
    watch_count=`grep $session_id $wchp_file | wc -l`
    echo "$session_id: $watch_count"
    total_watch_count=`expr $total_watch_count + $watch_count`
  fi
done < $cons_file
echo "total watcher count: $total_watch_count"


