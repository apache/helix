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

script_dir=`dirname $0`
LIB=$script_dir/../lib
CLASSPATH=$script_dir/../target/classes:"$LIB"/helix-core-0.1-SNAPSHOT-incubating.jar:"$LIB"/rabbitmq-client.jar:"$LIB"/commons-cli-1.1.jar:"$LIB"/commons-io-1.2.jar:"$LIB"/commons-math-2.1.jar:"$LIB"/jackson-core-asl-1.8.5.jar:"$LIB"/jackson-mapper-asl-1.8.5.jar:"$LIB"/log4j-1.2.15.jar:"$LIB"/org.restlet-1.1.10.jar:"$LIB"/zkclient-0.1.jar:"$LIB"/zookeeper-3.3.4.jar
# echo $CLASSPATH

java -cp "$CLASSPATH" org.apache.helix.filestore.SetupConsumerCluster $@
