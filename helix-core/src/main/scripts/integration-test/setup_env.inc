# set up env, figure out the path
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

ROOT_DIR=../../../
PATH_PREFIX_DIR=../../../integration-test
SCRIPT_DIR=$PATH_PREFIX_DIR/script/
CONFIG_DIR=integration-test/config
VAR_DIR=$PATH_PREFIX_DIR/var
LOG_DIR=$VAR_DIR/log
WORK_DIR=$WORK_DIR/work
WORK_DIR_FROM_ROOT=integration-test/var/work
LOG_DIR_FROM_ROOT=integration-test/var/log
DATA_DIR=$PATH_PREFIX_DIR/data
# solaris tail
TAIL_PATH=/usr/xpg4/bin/tail     
if [ ! -f $TAIL_PATH ]; then
  TAIL_PATH=tail
fi
# over all stats
all_stat=0
