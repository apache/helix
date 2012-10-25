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

#ROOT_DIR=../../../
export PATH_PREFIX_DIR=../
export SCRIPT_DIR=$PATH_PREFIX_DIR/script/
export VAR_DIR=$PATH_PREFIX_DIR/var
export DATA_DIR=$PATH_PREFIX_DIR/data

export SCRIPT_DIR=../script
export CONFIG_DIR=integration-test/config
#export VAR_DIR=../var

DEFAULT_TEST_NAME=`basename $0`

if [ -z "${TEST_NAME}" -o "${TEST_NAME}" = "-bash" ] ; then
  TEST_NAME=${DEFAULT_TEST_NAME}
fi

export TEST_NAME
export VAR_WORK_DIR=../var/work
export VAR_WORK_DIR_FROM_ROOT=intergration-test/var/work

if [ -z "${TEST_NAME}" ] ; then
  export LOG_DIR=../var/log
  export WORK_DIR=../var/work
  export WORK_DIR_FROM_ROOT=integration-test/var/work
  export LOG_DIR_FROM_ROOT=integration-test/var/log
else
  export LOG_DIR=../var/log/${TEST_NAME}
  export WORK_DIR=../var/work/${TEST_NAME}
  export WORK_DIR_FROM_ROOT=integration-test/var/work/${TEST_NAME}
  export LOG_DIR_FROM_ROOT=integration-test/var/log/${TEST_NAME}
fi
#export DATA_DIR=../data
export VIEW_ROOT=`cd ../../; echo $PWD`
# solaris tail
export TAIL_PATH=/usr/xpg4/bin/tail
if [ ! -f $TAIL_PATH ]; then
  export TAIL_PATH=tail
fi
# over all stats
all_stat=0
