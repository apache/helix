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

update_pom_version() {
  pom=$1
  version=$2
  echo "bump up $pom"
  sed -i'' -e "s/${version}/${new_version}/g" "$pom"
  if ! grep -C 1 "$new_version" $pom; then
    echo "Failed to update new version $new_version in $pom"
    exit 1
  fi
}

update_ivy() {
  module=$1
  ivy_file=`find $module -type f -name '*.ivy'`
  new_ivy_file="$module/$module-$new_version.ivy"
  if [ -f $ivy_file ]; then
    echo "bump up $ivy_file"
    git mv "$ivy_file" "$new_ivy_file"
    current_ivy_version=`get_version_from_ivy $new_ivy_file`
    sed -i'' -e "s/${current_ivy_version}/${new_version}/g" "$new_ivy_file"
    if ! grep -C 1 "$new_version" "$new_ivy_file"; then
      echo "Failed to update new version $new_version in $new_ivy_file"
      exit 1
    fi
  else
    echo "$module/$ivy_file not exist"
  fi
}

get_version_from_pom() {
  grep -A 1 "<artifactId>helix</artifactId>" $1 |tail -1 | awk 'BEGIN {FS="[<,>]"};{print $3}'
}

get_version_from_ivy() {
  grep revision "$1" | awk 'BEGIN {FS="[=,\"]"};{print $3}'
}

current_version=`get_version_from_pom pom.xml`
echo There are $# arguments to $0: $*
if [ "$#" -eq 1 ]; then
  new_version=$1
elif [ "$#" -eq 2 ]; then
  new_version=$2
else
  minor_version=`echo $current_version | cut -d'.' -f3 | cut -d'-' -f1`
  major_version=`echo $current_version | cut -d'.' -f1`
  submajor_version=`echo $current_version | cut -d'.' -f2`

  new_minor_version=`expr $minor_version + 1`
  new_version="$major_version.$submajor_version.$new_minor_version"
fi
echo "bump up: $current_version -> $new_version"
update_pom_version "pom.xml" $current_version

for module in "metrics-common" "metadata-store-directory-common" "zookeeper-api" "helix-common" "helix-core" \
              "helix-admin-webapp" "helix-rest" "helix-lock" "helix-view-aggregator" "helix-agent"; do
  update_ivy $module
  update_pom_version $module/pom.xml $current_version
done

for module in recipes/task-execution recipes helix-front \
           recipes/distributed-lock-manager recipes/rsync-replicated-file-system \
           recipes/rabbitmq-consumer-group recipes/service-discovery; do
  update_pom_version $module/pom.xml $current_version
done

#END
