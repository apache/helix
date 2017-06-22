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
  <title>Tutorial - Logical Accessors</title>
</head>

## [Helix Tutorial](./Tutorial.html): Logical Accessors

Helix constructs follow a logical hierarchy. A cluster contains participants, and serve logical resources. Each resource can be divided into partitions, which themselves can be replicated. Helix now supports configuring and modifying clusters programmatically in a hierarchical way using logical accessors.

[Click here](http://helix.apache.org/javadocs/0.7.0-incubating/reference/org/apache/helix/api/accessor/package-summary.html) for the Javadocs of the accessors.

### An Example

#### Configure a Participant

A participant is a combination of a host, port, and a UserConfig. A UserConfig is an arbitrary set of properties a Helix user can attach to any participant.

```
ParticipantId participantId = ParticipantId.from("localhost_12345");
ParticipantConfig participantConfig = new ParticipantConfig.Builder(participantId)
    .hostName("localhost").port(12345).build();
```

#### Configure a Resource

##### RebalancerContext
A Resource is essentially a combination of a RebalancerContext and a UserConfig. A [RebalancerContext](http://helix.apache.org/javadocs/0.7.0-incubating/reference/org/apache/helix/controller/rebalancer/context/RebalancerContext.html) consists of all the key properties required to rebalance a resource, including how it is partitioned and replicated, and what state model it follows. Most Helix resources will make use of a [PartitionedRebalancerContext](http://helix.apache.org/javadocs/0.7.0-incubating/reference/org/apache/helix/controller/rebalancer/context/PartitionedRebalancerContext.html), which is a RebalancerContext for resources that are partitioned.

Recall that there are four [rebalancing modes](./tutorial_rebalance.html) that Helix provides, and so Helix also provides the following subclasses for PartitionedRebalancerContext:

* [FullAutoRebalancerContext](http://helix.apache.org/javadocs/0.7.0-incubating/reference/org/apache/helix/controller/rebalancer/context/FullAutoRebalancerContext.html) for FULL_AUTO mode.
* [SemiAutoRebalancerContext](http://helix.apache.org/javadocs/0.7.0-incubating/reference/org/apache/helix/controller/rebalancer/context/SemiAutoRebalancerContext.html) for SEMI_AUTO mode. This class allows a user to specify "preference lists" to indicate where each partition should ideally be served
* [CustomRebalancerContext](http://helix.apache.org/javadocs/0.7.0-incubating/reference/org/apache/helix/controller/rebalancer/context/CustomRebalancerContext.html) for CUSTOMIZED mode. This class allows a user tp specify "preference maps" to indicate the location and state for each partition replica.

Helix also supports arbitrary subclasses of PartitionedRebalancerContext and even arbitrary implementations of RebalancerContext for applications that need a user-defined approach for rebalancing. For more, see [User-Defined Rebalancing](./tutorial_user_def_rebalancer.html)

##### In Action

Here is an example of a configured resource with a rebalancer context for FULL_AUTO mode and two partitions:

```
ResourceId resourceId = ResourceId.from("sampleResource");
StateModelDefinition stateModelDef = getStateModelDef();
Partition partition1 = new Partition(PartitionId.from(resourceId, "1"));
Partition partition2 = new Partition(PartitionId.from(resourceId, "2"));
FullAutoRebalancerContext rebalanceContext =
    new FullAutoRebalancerContext.Builder(resourceId).replicaCount(1).addPartition(partition1)
        .addPartition(partition2).stateModelDefId(stateModelDef.getStateModelDefId()).build();
ResourceConfig resourceConfig =
    new ResourceConfig.Builder(resourceId).rebalancerContext(rebalanceContext).build();
```

#### Add the Cluster

Now we can take the participant and resource configured above, add them to a cluster configuration, and then persist the entire cluster at once using a ClusterAccessor:

```
// configure the cluster
ClusterId clusterId = ClusterId.from("sampleCluster");
ClusterConfig clusterConfig = new ClusterConfig.Builder(clusterId).addParticipant(participantConfig)
    .addResource(resourceConfig).addStateModelDefinition(stateModelDef).build();

// create the cluster using a ClusterAccessor
HelixConnection connection = new ZkHelixConnection(zkAddr);
connection.connect();
ClusterAccessor clusterAccessor = connection.createClusterAccessor(clusterId);
clusterAccessor.createCluster(clusterConfig);
```

### Create, Read, Update, and Delete

Note that you don't have to specify the entire cluster beforehand! Helix provides a ClusterAccessor, ParticipantAccessor, and ResourceAccessor to allow changing as much or as little of the cluster as needed on the fly. You can add a resource or participant to a cluster, reconfigure a resource, participant, or cluster, remove components from the cluster, and more. See the [Javadocs](http://helix.apache.org/javadocs/0.7.0-incubating/reference/org/apache/helix/api/accessor/package-summary.html) to see all that the accessor classes can do.

#### Delta Classes

Updating a cluster, participant, or resource should involve selecting the element to change, and then letting Helix change only that component. To do this, Helix has included Delta classes for ClusterConfig, ParticipantConfig, and ResourceConfig.

#### Example: Updating a Participant

Tags are used for Helix depolyments where only certain participants can be allowed to serve certain resources. To do this, Helix only assigns resource replicas to participants who have a tag that the resource specifies. In this example, we will use ParticipantConfig.Delta to remove a participant tag and add another as part of a reconfiguration.

```
// specify the change to the participant
ParticipantConfig.Delta delta = new ParticipantConfig.Delta(participantId).addTag("newTag").removeTag("oldTag");

// update the participant configuration
ParticipantAccessor participantAccessor = connection.createParticipantAccessor(clusterId);
participantAccessor.updateParticipant(participantId, delta);
```

#### Example: Dropping a Resource
Removing a resource from the cluster is quite simple:

```
clusterAccessor.dropResourceFromCluster(resourceId);
```

#### Example: Reading the Cluster
Reading a full snapshot of the cluster is also a one-liner:

```
Cluster cluster = clusterAccessor.readCluster();
```

### Atomic Accessors

Helix also includes versions of ClusterAccessor, ParticipantAccessor, and ResourceAccessor that can complete operations atomically relative to one another. The specific semantics of the atomic operations are included in the Javadocs. These atomic classes should be used sparingly and only in cases where contention can adversely affect the correctness of a Helix-based cluster. For most deployments, this is not the case, and using these classes will cause a degradation in performance. However, the interface for all atomic accessors mirrors that of the non-atomic accessors.
