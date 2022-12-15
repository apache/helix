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


echo There are $# arguments to $0: $*
version=`grep -A 1 "<artifactId>helix</artifactId>" pom.xml |tail -1 | awk 'BEGIN {FS="[<,>]"};{print $3}'`
if [ "$#" -eq 1 ]; then
  new_version=$1
elif [ "$#" -eq 2 ]; then
  version=$1
  new_version=$2
else
  echo "ERROR: Need oldVersion and newVersion two arguments"
  exit 0
fi


# Move Ivy
mv helix-admin-webapp/helix-admin-webapp-$version-SNAPSHOT.ivy helix-admin-webapp/helix-admin-webapp-$new_version-SNAPSHOT.ivy
mv helix-agent/helix-agent-$version-SNAPSHOT.ivy helix-agent/helix-agent-$new_version-SNAPSHOT.ivy
mv helix-common/helix-common-$version-SNAPSHOT.ivy helix-common/helix-common-$new_version-SNAPSHOT.ivy
mv helix-core/helix-core-$version-SNAPSHOT.ivy helix-core/helix-core-$new_version-SNAPSHOT.ivy
mv helix-lock/helix-lock-$version-SNAPSHOT.ivy helix-lock/helix-lock-$new_version-SNAPSHOT.ivy
mv helix-rest/helix-rest-$version-SNAPSHOT.ivy helix-rest/helix-rest-$new_version-SNAPSHOT.ivy
mv metadata-store-directory-common/metadata-store-directory-common-$version-SNAPSHOT.ivy metadata-store-directory-common/metadata-store-directory-common-$new_version-SNAPSHOT.ivy
mv metrics-common/metrics-common-$version-SNAPSHOT.ivy metrics-common/metrics-common-$new_version-SNAPSHOT.ivy
mv zookeeper-api/zookeeper-api-$version-SNAPSHOT.ivy zookeeper-api/zookeeper-api-$new_version-SNAPSHOT.ivy
mv helix-view-aggregator/helix-view-aggregator-$version-SNAPSHOT.ivy helix-view-aggregator/helix-view-aggregator-$new_version-SNAPSHOT.ivy


find . -type f -name '*.ivy' -exec sed -i "s/$version/$new_version/g" {} \;
find . -type f -name 'pom.xml' -exec sed -i "s/$version/$new_version/g" {} \;



echo Bump up is DONE! Please double check and commit!
#END
