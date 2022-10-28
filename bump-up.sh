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
  echo "bump up $pom"
  version=`grep "<revision>" $pom | awk 'BEGIN {FS="[<,>]"};{print $3}'`
  sed -i'' -e "s/<revision>$version/<revision>$new_version/g" $pom
  if ! grep -C 1 "$new_version" $pom; then
    echo "Failed to update new version $new_version in $pom"
    exit 1
  fi
}

update_ivy() {
  module=$1
  ivy_file="$module-$current_version.ivy"
  new_ivy_file="$module-$new_version.ivy"
  if [ -f $module/$ivy_file ]; then
    echo "bump up $module/$ivy_file"
    git mv "$module/$ivy_file" "$module/$new_ivy_file"
    sed -i'' -e "s/${current_version}/${new_version}/g" "$module/$new_ivy_file"
    if ! grep -C 1 "$new_version" "$module/$new_ivy_file"; then
      echo "Failed to update new version $new_version in $module/$new_ivy_file"
      exit 1
    fi
  else
    echo "$module/$ivy_file not exist"
  fi
}

current_version=`grep "<revision>" pom.xml | awk 'BEGIN {FS="[<,>]"};{print $3}'`
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
update_pom_version "pom.xml"

for module in "metrics-common" "metadata-store-directory-common" "zookeeper-api" "helix-common" "helix-core" \
              "helix-admin-webapp" "helix-front" "helix-rest" "helix-lock" "helix-view-aggregator" "helix-agent"; do
  update_ivy $module
done

#END
