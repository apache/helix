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
  <title>Tutorial - Spectator</title>
</head>

## [Helix Tutorial](./Tutorial.html): Spectator

Next, we\'ll learn how to implement a __spectator__.  Typically, a spectator needs to react to changes within the distributed system.  Examples: a client that needs to know where to send a request, a topic consumer in a consumer group.  The spectator is automatically informed of changes in the _external state_ of the cluster, but it does not have to add any code to keep track of other components in the system.

### Start a Connection

Same as for a participant, The Helix manager is the common component that connects each system component with the cluster.

It requires the following parameters:

* clusterName: A logical name to represent the group of nodes
* instanceName: A logical name of the process creating the manager instance. Generally this is host:port
* instanceType: Type of the process. This can be one of the following types, in this case, use SPECTATOR:
    * CONTROLLER: Process that controls the cluster, any number of controllers can be started but only one will be active at any given time
    * PARTICIPANT: Process that performs the actual task in the distributed system
    * SPECTATOR: Process that observes the changes in the cluster
    * ADMIN: To carry out system admin actions
* zkConnectString: Connection string to ZooKeeper. This is of the form host1:port1,host2:port2,host3:port3

After the Helix manager instance is created, the only thing that needs to be registered is the listener.  When the ExternalView changes, the listener is notified.

A spectator observes the cluster and is notified when the state of the system changes. Helix consolidates the state of entire cluster in one Znode called ExternalView.
Helix provides a default implementation RoutingTableProvider that caches the cluster state and updates it when there is a change in the cluster.

```
manager = HelixManagerFactory.getZKHelixManager(clusterName,
                                                instanceName,
                                                InstanceType.SPECTATOR,
                                                zkConnectString);
manager.connect();
RoutingTableProvider routingTableProvider = new RoutingTableProvider();
manager.addExternalViewChangeListener(routingTableProvider);
```

### Spectator Code

In the following code snippet, the application sends the request to a valid instance by interrogating the external view.  Suppose the desired resource for this request is in the partition myDB_1.

```
// instances = routingTableProvider.getInstances(, "PARTITION_NAME", "PARTITION_STATE");
instances = routingTableProvider.getInstances("myDB", "myDB_1", "ONLINE");

////////////////////////////////////////////////////////////////////////////////////////////////
// Application-specific code to send a request to one of the instances                        //
////////////////////////////////////////////////////////////////////////////////////////////////

theInstance = instances.get(0);  // should choose an instance and throw an exception if none are available
result = theInstance.sendRequest(yourApplicationRequest, responseObject);

```

When the external view changes, the application needs to react by sending requests to a different instance.
