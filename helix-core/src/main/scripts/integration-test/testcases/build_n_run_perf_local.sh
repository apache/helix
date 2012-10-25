#!/bin/sh
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

cd ../../../../../
mvn clean package -Dmaven.test.skip.exec=true
cd -
chmod +x ../../../../../target/helix-core-pkg/bin/*
./helix_random_kill_local_nojrat.test
# ./analyze-zklog_local.sh ~/zookeeper/server1/data/ > /tmp/helix_perf_`date '+%y%m%d_%H%m%d%s.log'`
# ls -ltr /tmp/helix_perf_* | tail -1 |awk '{print $9}'
# ls -ltr /tmp/helix_perf_* | tail -1 | awk '{print $9}' | xargs grep latency
