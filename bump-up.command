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
  version=$1
  new_version=$2
else
  version=`grep -A 1 "<artifactId>helix</artifactId>" pom.xml | grep "<version>" | awk 'BEGIN {FS="[<,>]"};{print $3}'`
  minor_version=`echo $version | cut -d'.' -f3`
  major_version=`echo $version | cut -d'.' -f1` # should be 0
  submajor_version=`echo $version | cut -d'.' -f2`

  new_minor_version=`expr $minor_version + 1`
#  new_version=`echo $version | sed -e "s/${minor_version}/${new_minor_version}/g"`
  new_version="$major_version.$submajor_version.$new_minor_version"
fi
cecho "bump up: $version -> $new_version" $red

#: <<'END'
cecho "bump up pom.xml" $green
sed -i "s/${version}/${new_version}/g" pom.xml
# git diff pom.xml
grep -C 1 "$new_version" pom.xml

cecho "bump up helix-core/pom.xml" $green
sed -i "s/${version}/${new_version}/g" helix-core/pom.xml
grep -C 1 "$new_version" helix-core/pom.xml
# git diff helix-core/pom.xml

ivy_file="helix-core-"$version".ivy"
new_ivy_file="helix-core-"$new_version".ivy"
# echo "$ivy_file"
if [ -f helix-core/$ivy_file ]; then
  cecho "bump up helix-core/$ivy_file" $green
  git mv "helix-core/$ivy_file" "helix-core/$new_ivy_file"
  sed -i "s/${version}/${new_version}/g" "helix-core/$new_ivy_file"
  grep -C 1 "$new_version" "helix-core/$new_ivy_file"
else
  cecho "helix-core/$ivy_file not exist" $red
fi

cecho "bump up helix-admin-webapp/pom.xml" $green
sed -i "s/${version}/${new_version}/g" helix-admin-webapp/pom.xml
grep -C 1 "$new_version" helix-admin-webapp/pom.xml
# git diff helix-core/pom.xml

ivy_file="helix-admin-webapp-"$version".ivy"
new_ivy_file="helix-admin-webapp-"$new_version".ivy"
# echo "$ivy_file"
if [ -f helix-admin-webapp/$ivy_file ]; then
  cecho "bump up helix-admin-webapp/$ivy_file" $green
  git mv "helix-admin-webapp/$ivy_file" "helix-admin-webapp/$new_ivy_file"
  sed -i "s/${version}/${new_version}/g" "helix-admin-webapp/$new_ivy_file"
  grep -C 1 "$new_version" "helix-admin-webapp/$new_ivy_file"
else
  cecho "helix-admin-webapp/$ivy_file not exist" $red
fi

for POM in helix-agent/pom.xml recipes/task-execution/pom.xml recipes/pom.xml recipes/distributed-lock-manager/pom.xml recipes/rsync-replicated-file-system/pom.xml recipes/rabbitmq-consumer-group/pom.xml recipes/service-discovery/pom.xml
do
  cecho "bump up $POM" $green
  sed -i "s/${version}/${new_version}/g" $POM 
  grep -C 1 "$new_version" $POM
done

#END


