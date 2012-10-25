#!/bin/bash
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

# users/machines/dirs info for each test machine
USER_TAB=( "zzhang" "zzhang" "zzhang" )
# MACHINE_TAB=( "eat1-app20.corp" "eat1-app21.corp" "eat1-app22.corp" )
MACHINE_TAB=( "eat1-app26.corp" "eat1-app27.corp" "eat1-app28.corp" )

# SCRIPT_DIR_TAB=( "/export/home/zzhang/workspace/helix/helix-core/src/main/scripts/integration-test/script" "/export/home/zzhang/workspace/helix/helix-core/src/main/scripts/integration-test/script" "/export/home/zzhang/workspace/helix/helix-core/src/main/scripts/integration-test/script" "/export/home/zzhang/workspace/helix/helix-core/src/main/scripts/integration-test/script" )

# constants
machine_nb=${#MACHINE_TAB[*]}

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

# : <<'END'
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 zklog_dir test_start_time (yyMMdd_hhmmss_SSS)"
  exit
fi

../../../../../target/helix-core-pkg/bin/zk-log-analyzer $1 test-cluster $2

# END
