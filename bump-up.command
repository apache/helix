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
echo "bump up: $version -> $new_version"

#: <<'END'
echo "bump up pom.xml"
sed -i "s/${version}/${new_version}/g" pom.xml
# git diff pom.xml
grep -C 1 "$new_version" pom.xml

echo "bump up helix-core/pom.xml"
sed -i "s/${version}/${new_version}/g" helix-core/pom.xml
grep -C 1 "$new_version" helix-core/pom.xml
# git diff helix-core/pom.xml

ivy_file="helix-core-"$version".ivy"
new_ivy_file="helix-core-"$new_version".ivy"
# echo "$ivy_file"
if [ -f helix-core/$ivy_file ]; then
  echo "bump up helix-core/$ivy_file"
  git mv "helix-core/$ivy_file" "helix-core/$new_ivy_file"
  sed -i "s/${version}/${new_version}/g" "helix-core/$new_ivy_file"
  grep -C 1 "$new_version" "helix-core/$new_ivy_file"
else
  echo "helix-core/$ivy_file not exist"
fi

echo "bump up helix-admin-webapp/pom.xml"
sed -i "s/${version}/${new_version}/g" helix-admin-webapp/pom.xml
grep -C 1 "$new_version" helix-admin-webapp/pom.xml
# git diff helix-admin-webapp/pom.xml

ivy_file="helix-admin-webapp-"$version".ivy"
new_ivy_file="helix-admin-webapp-"$new_version".ivy"
# echo "$ivy_file"
if [ -f helix-admin-webapp/$ivy_file ]; then
  echo "bump up helix-admin-webapp/$ivy_file"
  git mv "helix-admin-webapp/$ivy_file" "helix-admin-webapp/$new_ivy_file"
  sed -i "s/${version}/${new_version}/g" "helix-admin-webapp/$new_ivy_file"
  grep -C 1 "$new_version" "helix-admin-webapp/$new_ivy_file"
else
  echo "helix-admin-webapp/$ivy_file not exist"
fi

echo "bump up helix-rest/pom.xml"
sed -i "s/${version}/${new_version}/g" helix-rest/pom.xml
grep -C 1 "$new_version" helix-rest/pom.xml
# git diff helix-rest/pom.xml

ivy_file="helix-rest-"$version".ivy"
new_ivy_file="helix-rest-"$new_version".ivy"
# echo "$ivy_file"
if [ -f helix-rest/$ivy_file ]; then
  echo "bump up helix-rest/$ivy_file"
  git mv "helix-rest/$ivy_file" "helix-rest/$new_ivy_file"
  sed -i "s/${version}/${new_version}/g" "helix-rest/$new_ivy_file"
  grep -C 1 "$new_version" "helix-rest/$new_ivy_file"
else
  echo "helix-rest/$ivy_file not exist"
fi

for POM in helix-agent/pom.xml recipes/task-execution/pom.xml recipes/pom.xml recipes/distributed-lock-manager/pom.xml recipes/rsync-replicated-file-system/pom.xml recipes/rabbitmq-consumer-group/pom.xml recipes/service-discovery/pom.xml
do
  echo "bump up $POM"
  sed -i "s/${version}/${new_version}/g" $POM 
  grep -C 1 "$new_version" $POM
done

echo "bump up helix-front/pom.xml"
sed -i "s/${version}/${new_version}/g" helix-front/pom.xml
grep -C 1 "$new_version" helix-front/pom.xml

#END


