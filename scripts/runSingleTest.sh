#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ ! $1 ] ;then
  echo "No testName provided!
Example: script.sh <testClass[#testMethod]> [repeat-time] [project]"
  exit
fi
testName=$1

if [ ! $2 ] ;then
  repeated=-1
else
  repeated=$2
fi

if [ ! $3 ] ;then
  project="helix-core"
else
  project=$3
fi

echo "Running test on $testName in component $project for $repeated times."
for (( i = 1; $repeated < 0 || i <= $repeated; i++ ))
do
  echo "======================================================================
Attempt $i $testName
======================================================================"
  mvn test -o -Dtest=$testName -pl=$project
  exitcode=$?
  if [ $exitcode -ne 0 ]
  then
    echo "======================================================================
Error at attempt $i
======================================================================"
    break
  fi
done


