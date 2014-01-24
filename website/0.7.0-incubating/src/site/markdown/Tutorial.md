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
  <title>Tutorial</title>
</head>

# Helix Tutorial

In this tutorial, we will cover the roles of a Helix-managed cluster, and show the code you need to write to integrate with it.  In many cases, there is a simple default behavior that is often appropriate, but you can also customize the behavior.

Convention: we first cover the _basic_ approach, which is the easiest to implement.  Then, we'll describe _advanced_ options, which give you more control over the system behavior, but require you to write more code.


### Prerequisites

1. Read [Concepts/Terminology](../Concepts.html) and [Architecture](../Architecture.html)
2. Read the [Quickstart guide](./Quickstart.html) to learn how Helix models and manages a cluster
3. Install Helix source.  See: [Quickstart](./Quickstart.html) for the steps.

### Tutorial Outline

1. [Participant](./tutorial_participant.html)
2. [Spectator](./tutorial_spectator.html)
3. [Controller](./tutorial_controller.html)
4. [Rebalancing Algorithms](./tutorial_rebalance.html)
5. [User-Defined Rebalancing](./tutorial_user_def_rebalancer.html)
6. [State Machines](./tutorial_state.html)
7. [Messaging](./tutorial_messaging.html)
8. [Customized health check](./tutorial_health.html)
9. [Throttling](./tutorial_throttling.html)
10. [Application Property Store](./tutorial_propstore.html)
11. [Logical Accessors](./tutorial_accessors.html)
12. [Admin Interface](./tutorial_admin.html)
13. [YAML Cluster Setup](./tutorial_yaml.html)
14. [Helix Agent (for non-JVM systems)](./tutorial_agent.html)

### Preliminaries

First, we need to set up the system.  Let\'s walk through the steps in building a distributed system using Helix. We will show how to do this using both the Java admin interface, as well as the [cluster accessor](./tutorial_accessors.html) interface. You can choose either interface depending on which most closely matches your needs.

#### Start ZooKeeper

This starts a zookeeper in standalone mode. For production deployment, see [Apache ZooKeeper](http://zookeeper.apache.org) for instructions.

```
./start-standalone-zookeeper.sh 2199 &
```

#### Create a Cluster

Creating a cluster will define the cluster in appropriate ZNodes on ZooKeeper.

Using the Java accessor API:

```
// Note: ZK_ADDRESS is the host:port of Zookeeper
String ZK_ADDRESS = "localhost:2199";
HelixConnection connection = new ZKHelixConnection(ZK_ADDRESS);

ClusterId clusterId = ClusterId.from("helix-demo");
ClusterAccessor clusterAccessor = connection.createClusterAccessor(clusterId);
ClusterConfig clusterConfig = new ClusterConfig.Builder(clusterId).build();
clusterAccessor.createCluster(clusterConfig);
```

OR

Using the HelixAdmin Java interface:

```
// Create setup tool instance
// Note: ZK_ADDRESS is the host:port of Zookeeper
String ZK_ADDRESS = "localhost:2199";
HelixAdmin admin = new ZKHelixAdmin(ZK_ADDRESS);

String CLUSTER_NAME = "helix-demo";
//Create cluster namespace in zookeeper
admin.addCluster(CLUSTER_NAME);
```

OR

Using the command-line interface:

```
./helix-admin.sh --zkSvr localhost:2199 --addCluster helix-demo
```


#### Configure the Nodes of the Cluster

First we\'ll add new nodes to the cluster, then configure the nodes in the cluster. Each node in the cluster must be uniquely identifiable.
The most commonly used convention is hostname_port.

```
int NUM_NODES = 2;
String hosts[] = new String[]{"localhost","localhost"};
int ports[] = new int[]{7000,7001};
for (int i = 0; i < NUM_NODES; i++)
{
  ParticipantId participantId = ParticipantId.from(hosts[i] + "_" + ports[i]);

  // set additional configuration for the participant; these can be accessed during node start up
  UserConfig userConfig = new UserConfig(Scope.participant(participantId));
  userConfig.setSimpleField("key", "value");

  // configure and add the participant
  ParticipantConfig participantConfig = new ParticipantConfig.Builder(participantId)
      .hostName(hosts[i]).port(ports[i]).enabled(true).userConfig(userConfig).build();
  clusterAccessor.addParticipantToCluster(participantConfig);
}
```

OR

Using the HelixAdmin Java interface:

```
String CLUSTER_NAME = "helix-demo";
int NUM_NODES = 2;
String hosts[] = new String[]{"localhost","localhost"};
String ports[] = new String[]{7000,7001};
for (int i = 0; i < NUM_NODES; i++)
{
  InstanceConfig instanceConfig = new InstanceConfig(hosts[i] + "_" + ports[i]);
  instanceConfig.setHostName(hosts[i]);
  instanceConfig.setPort(ports[i]);
  instanceConfig.setInstanceEnabled(true);

  //Add additional system specific configuration if needed. These can be accessed during the node start up.
  instanceConfig.getRecord().setSimpleField("key", "value");
  admin.addInstance(CLUSTER_NAME, instanceConfig);
}
```

#### Configure the Resource

A _resource_ represents the actual task performed by the nodes. It can be a database, index, topic, queue or any other processing entity.
A _resource_ can be divided into many sub-parts known as _partitions_.


##### Define the State Model and Constraints

For scalability and fault tolerance, each partition can have one or more replicas.
The __state model__ allows one to declare the system behavior by first enumerating the various STATES, and the TRANSITIONS between them.
A simple model is ONLINE-OFFLINE where ONLINE means the task is active and OFFLINE means it\'s not active.
You can also specify how many replicas must be in each state, these are known as __constraints__.
For example, in a search system, one might need more than one node serving the same index to handle the load.

The allowed states:

* MASTER
* SLAVE
* OFFLINE

The allowed transitions:

* OFFLINE to SLAVE
* SLAVE to OFFLINE
* SLAVE to MASTER
* MASTER to SLAVE

The constraints:

* no more than 1 MASTER per partition
* the rest of the replicas should be slaves

The following snippet shows how to declare the _state model_ and _constraints_ for the MASTER-SLAVE model.

```
StateModelDefinition.Builder builder = new StateModelDefinition.Builder(STATE_MODEL_NAME);

// Add states and their rank to indicate priority. A lower rank corresponds to a higher priority
builder.addState(MASTER, 1);
builder.addState(SLAVE, 2);
builder.addState(OFFLINE);

// Set the initial state when the node starts
builder.initialState(OFFLINE);

// Add transitions between the states.
builder.addTransition(OFFLINE, SLAVE);
builder.addTransition(SLAVE, OFFLINE);
builder.addTransition(SLAVE, MASTER);
builder.addTransition(MASTER, SLAVE);

// set constraints on states.

// static constraint: upper bound of 1 MASTER
builder.upperBound(MASTER, 1);

// dynamic constraint: R means it should be derived based on the replication factor for the cluster
// this allows a different replication factor for each resource without
// having to define a new state model
//
builder.dynamicUpperBound(SLAVE, "R");
StateModelDefinition statemodelDefinition = builder.build();
```

Then, add the state model definition:

```
clusterAccessor.addStateModelDefinitionToCluster(stateModelDefinition);
```

OR

```
admin.addStateModelDef(CLUSTER_NAME, STATE_MODEL_NAME, stateModelDefinition);
```

##### Assigning Partitions to Nodes

The final goal of Helix is to ensure that the constraints on the state model are satisfied.
Helix does this by assigning a __state__ to a partition (such as MASTER, SLAVE), and placing it on a particular node.

There are 3 assignment modes Helix can operate on

* FULL_AUTO: Helix decides the placement and state of a partition.
* SEMI_AUTO: Application decides the placement but Helix decides the state of a partition.
* CUSTOMIZED: Application controls the placement and state of a partition.

For more info on the assignment modes, see [Rebalancing Algorithms](./tutorial_rebalance.html) section of the tutorial.

Here is an example of adding the resource in SEMI_AUTO mode (i.e. locations of partitions are specified a priori):

```
int NUM_PARTITIONS = 6;
int NUM_REPLICAS = 2;
ResourceId resourceId = resourceId.from("MyDB");

SemiAutoRebalancerContext context = new SemiAutoRebalancerContext.Builder(resourceId)
  .replicaCount(NUM_REPLICAS).addPartitions(NUM_PARTITIONS)
  .stateModelDefId(stateModelDefinition.getStateModelDefId())
  .addPreferenceList(partition1Id, preferenceList) // preferred locations of each partition
  // add other preference lists per partition
  .build();

// or add all preference lists at once if desired (map of PartitionId to List of ParticipantId)
context.setPreferenceLists(preferenceLists);

// or generate a default set of preference lists given the set of all participants
context.generateDefaultConfiguration(stateModelDefinition, participantIdSet);

// add the resource to the cluster
ResourceConfig resourceConfig = new ResourceConfig.Builder(resourceId)
  .rebalancerContext(context)
  .build();
clusterAccessor.addResourceToCluster(resourceConfig);
```

OR

```
String RESOURCE_NAME = "MyDB";
int NUM_PARTITIONS = 6;
String MODE = "SEMI_AUTO";
int NUM_REPLICAS = 2;

admin.addResource(CLUSTER_NAME, RESOURCE_NAME, NUM_PARTITIONS, STATE_MODEL_NAME, MODE);

// specify the preference lists yourself
IdealState idealState = admin.getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME);
idealState.setPreferenceList(partitionId, preferenceList); // preferred locations of each partition
// add other preference lists per partition

// or add all preference lists at once if desired
idealState.getRecord().setListFields(preferenceLists);
admin.setResourceIdealState(CLUSTER_NAME, RESOURCE_NAME, idealState);

// or generate a default set of preference lists
admin.rebalance(CLUSTER_NAME, RESOURCE_NAME, NUM_REPLICAS);
```

