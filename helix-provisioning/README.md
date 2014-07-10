<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

checkout helix provisioning branch
cd helix
mvn clean package -DskipTests
cd helix-provisioning


Download and install YARN start all services (datanode, resourcemanage, nodemanager, jobHistoryServer(optional))

Will post the instructions to get a local YARN cluster.

target/helix-provisioning-pkg/bin/app-launcher.sh org.apache.helix.provisioning.yarn.example.HelloWordAppSpecFactory /Users/kgopalak/Documents/projects/incubator-helix/helix-provisioning/src/main/resources/hello_world_app_spec.yaml





