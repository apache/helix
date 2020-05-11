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

<head>
  <title>Tutorial - Controller</title>
</head>

## [Helix Tutorial](./Tutorial.html): Controller

Next, let\'s implement the controller.  This is the brain of the cluster.  Helix makes sure there is exactly one active controller running the cluster.

### Start a Connection

The Helix manager requires the following parameters:

* clusterName: A logical name to represent the group of nodes
* instanceName: A logical name of the process creating the manager instance. Generally this is host:port
* instanceType: Type of the process. This can be one of the following types, in this case use CONTROLLER:
    * CONTROLLER: Process that controls the cluster, any number of controllers can be started but only one will be active at any given time
    * PARTICIPANT: Process that performs the actual task in the distributed system
    * SPECTATOR: Process that observes the changes in the cluster
    * ADMIN: To carry out system admin actions
* zkConnectString: Connection string to ZooKeeper. This is of the form host1:port1,host2:port2,host3:port3

```
manager = HelixManagerFactory.getZKHelixManager(clusterName,
                                                instanceName,
                                                instanceType,
                                                zkConnectString);
```

### Controller Code

The Controller needs to know about all changes in the cluster. Helix takes care of this with the default implementation.
If you need additional functionality, see GenericHelixController on how to configure the pipeline.

```
manager = HelixManagerFactory.getZKHelixManager(clusterName,
                                                instanceName,
                                                InstanceType.CONTROLLER,
                                                zkConnectString);
manager.connect();
```
The snippet above shows how the controller is started. You can also start the controller using command line interface.

```
cd helix/helix-core/target/helix-core-pkg/bin
./run-helix-controller.sh --zkSvr <Zookeeper ServerAddress (Required)>  --cluster <Cluster name (Required)>
```

### Controller Deployment Modes

Helix provides multiple options to deploy the controller.

#### STANDALONE

The Controller can be started as a separate process to manage a cluster. This is the recommended approach. However, since one controller can be a single point of failure, multiple controller processes are required for reliability.  Even if multiple controllers are running, only one will be actively managing the cluster at any time and is decided by a leader-election process. If the leader fails, another leader will take over managing the cluster.

Even though we recommend this method of deployment, it has the drawback of having to manage an additional service for each cluster. See the Controller as a Service option.

#### EMBEDDED

If setting up a separate controller process is not viable, then it is possible to embed the controller as a library in each of the participants.

#### CONTROLLER AS A SERVICE

One of the cool features we added in Helix was to use a set of controllers to manage a large number of clusters.

For example if you have X clusters to be managed, instead of deploying X*3 (3 controllers for fault tolerance) controllers for each cluster, one can deploy just 3 controllers.  Each controller can manage X/3 clusters.  If any controller fails, the remaining two will manage X/2 clusters.

Next, let\'s implement the controller.  This is the brain of the cluster.  Helix makes sure there is exactly one active controller running the cluster.

### Start the Helix agent


It requires the following parameters:

* clusterName: A logical name to represent the group of nodes
* instanceName: A logical name of the process creating the manager instance. Generally this is host:port.
* instanceType: Type of the process. This can be one of the following types, in this case use CONTROLLER:
    * CONTROLLER: Process that controls the cluster, any number of controllers can be started but only one will be active at any given time.
    * PARTICIPANT: Process that performs the actual task in the distributed system.
    * SPECTATOR: Process that observes the changes in the cluster.
    * ADMIN: To carry out system admin actions.
* zkConnectString: Connection string to Zookeeper. This is of the form host1:port1,host2:port2,host3:port3.

```
      manager = HelixManagerFactory.getZKHelixManager(clusterName,
                                                      instanceName,
                                                      instanceType,
                                                      zkConnectString);
```

### Controller Code

The Controller needs to know about all changes in the cluster. Helix takes care of this with the default implementation.
If you need additional functionality, see GenericHelixController on how to configure the pipeline.

```
      manager = HelixManagerFactory.getZKHelixManager(clusterName,
                                                          instanceName,
                                                          InstanceType.CONTROLLER,
                                                          zkConnectString);
     manager.connect();
     GenericHelixController controller = new GenericHelixController();
     manager.addConfigChangeListener(controller);
     manager.addLiveInstanceChangeListener(controller);
     manager.addIdealStateChangeListener(controller);
     manager.addExternalViewChangeListener(controller);
     manager.addControllerListener(controller);
```
The snippet above shows how the controller is started. You can also start the controller using command line interface.

```
cd helix/helix-core/target/helix-core-pkg/bin
./run-helix-controller.sh --zkSvr <Zookeeper ServerAddress (Required)>  --cluster <Cluster name (Required)>
```

### Controller Deployment Modes

Helix provides multiple options to deploy the controller.

#### STANDALONE

The Controller can be started as a separate process to manage a cluster. This is the recommended approach. However, since one controller can be a single point of failure, multiple controller processes are required for reliability.  Even if multiple controllers are running, only one will be actively managing the cluster at any time and is decided by a leader-election process. If the leader fails, another leader will take over managing the cluster.

Even though we recommend this method of deployment, it has the drawback of having to manage an additional service for each cluster. See Controller As a Service option.

#### EMBEDDED

If setting up a separate controller process is not viable, then it is possible to embed the controller as a library in each of the participants.

#### CONTROLLER AS A SERVICE

One of the cool features we added in Helix is to use a set of controllers to manage a large number of clusters.

For example if you have X clusters to be managed, instead of deploying X*3 (3 controllers for fault tolerance) controllers for each cluster, one can deploy just 3 controllers.  Each controller can manage X/3 clusters.  If any controller fails, the remaining two will manage X/2 clusters.
