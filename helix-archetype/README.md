<!--
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

helix-archetype
===============

A maven archetype that sets up a sample [Apache Helix](http://helix.apache.org/) project.

Generate
--------

First, clone this repository and install the archetype

```
git clone git@github.com:brandtg/helix-archetype.git
cd helix-archetype
mvn install
```

Then generate your application

```
mvn archetype:generate \
  -DarchetypeGroupId=org.apache.helix \
  -DarchetypeArtifactId=helix-archetype \
  -DarchetypeVersion=1.0-SNAPSHOT \
  -DgroupId=com.example \
  -DartifactId=my-app \
  -Dname=MyApp \
  -DinteractiveMode=false
```

This creates a simple OnlineOffline application, though it should be straightforward to change the state model by referencing the Helix documentation.

The `pom.xml` creates a single versioned artifact, which can run all of your cluster's roles via the `${name}Main` entry point.

This makes it a little nicer to manage multiple different cluster roles: just build this artifact, and deploy it everywhere with different CLI args.

Example
-------

To get started, build the artifact after generating your application:

```
cd my-app
mvn install
```

Run a ZooKeeper

```
java -jar target/my-app-1.0-SNAPSHOT.jar zookeeper 2191 /tmp/zk
```

Set up a cluster, then add a node and a resource with 4 partitions

```
java -jar target/my-app-1.0-SNAPSHOT.jar setup --zkSvr localhost:2191 --addCluster TEST_CLUSTER
java -jar target/my-app-1.0-SNAPSHOT.jar setup --zkSvr localhost:2191 --addNode TEST_CLUSTER node0
java -jar target/my-app-1.0-SNAPSHOT.jar setup --zkSvr localhost:2191 --addResource TEST_CLUSTER test 4 OnlineOffline
```

Run a controller

```
java -jar target/my-app-1.0-SNAPSHOT.jar controller --zkSvr localhost:2191 --cluster TEST_CLUSTER
```

Run a participant (note: does nothing)

```
java -jar target/my-app-1.0-SNAPSHOT.jar participant localhost:2181 TEST_CLUSTER node0
```

Rebalance the resource (should work...)

```
java -jar target/my-app-1.0-SNAPSHOT.jar setup  --zkSvr localhost:2191 --rebalance TEST_CLUSTER test 1
```
