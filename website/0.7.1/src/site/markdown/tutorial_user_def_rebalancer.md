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
  <title>Tutorial - User-Defined Rebalancing</title>
</head>

## [Helix Tutorial](./Tutorial.html): User-Defined Rebalancing

Even though Helix can compute both the location and the state of replicas internally using a default fully-automatic rebalancer, specific applications may require rebalancing strategies that optimize for different requirements. Thus, Helix allows applications to plug in arbitrary rebalancer algorithms that implement a provided interface. One of the main design goals of Helix is to provide maximum flexibility to any distributed application. Thus, it allows applications to fully implement the rebalancer, which is the core constraint solver in the system, if the application developer so chooses.

Whenever the state of the cluster changes, as is the case when participants join or leave the cluster, Helix automatically calls the rebalancer to compute a new mapping of all the replicas in the resource. When using a pluggable rebalancer, the only required step is to register it with Helix. Subsequently, no additional bootstrapping steps are necessary. Helix uses reflection to look up and load the class dynamically at runtime. As a result, it is also technically possible to change the rebalancing strategy used at any time.

The [HelixRebalancer](http://helix.apache.org/javadocs/0.7.1/reference/org/apache/helix/controller/rebalancer/HelixRebalancer.html) interface is as follows:

```
public void init(HelixManager helixManager, ControllerContextProvider contextProvider);

public ResourceAssignment computeResourceMapping(IdealState idealState, RebalancerConfig config,
    ResourceAssignment, prevAssignment, Cluster cluster, ResourceCurrentState currentState);
```

In ```init()```, the first parameter is a reference to the controller's connection, and the second is a helper that allows a rebalancer to persist arbitrary context for future lookup.

In ```rebalance()```, the first parameter is a configuration of the resource to rebalance, the second is optional user-provided configuration for the rebalancer, the third is the output of the previous invocation of this method (if supported), the the fourth is a full cache of all of the cluster data available to Helix, and the fifth is a snapshot of the actual current placements and state assignments. Internally, Helix implements the same interface for its own rebalancing routines, so a user-defined rebalancer will be cognizant of the same information about the cluster as an internal implementation. Helix strives to provide applications the ability to implement algorithms that may require a large portion of the entire state of the cluster to make the best placement and state assignment decisions possible.

A ResourceAssignment is a full representation of the location and the state of each replica of each partition of a given resource. This is a simple representation of the placement that the algorithm believes is the best possible. If the placement meets all defined constraints, this is what will become the actual state of the distributed system.

### Rebalancer Config

Helix provides an interface called [RebalancerConfig](http://helix.apache.org/javadocs/0.7.1/reference/org/apache/helix/controller/rebalancer/config/RebalancerConfig.html). In practice, this may not be necessary for most rebalancers, but it allows maximum flexibility in specifying how a rebalancer should behave.

### Specifying a Rebalancer

To specify the rebalancer, one can use ```IdealState#setRebalancerRef(RebalancerRef)``` to specify the specific implementation of the rebalancer class:

```
IdealState idealState = helixAdmin.getResourceIdealState(clusterName, resourceName);
idealState.setRebalanceMode(RebalanceMode.USER_DEFINED);
idealState.setRebalancerRef(RebalancerRef.from(MyRebalancer.class));
```
There are two key fields to set to specify that a pluggable rebalancer should be used. First, the rebalance mode should be set to USER_DEFINED, and second the rebalancer class name should be set to a class that implements Rebalancer and is within the scope of the project. The class name is a fully-qualified class name consisting of its package and its name.

#### Using YAML
Alternatively, the rebalancer class name can be specified in a YAML file representing the cluster configuration. The requirements are the same, but the representation is more compact. Below are the first few lines of an example YAML file. To see a full YAML specification, see the [YAML tutorial](./tutorial_yaml.html).

```
clusterName: lock-manager-custom-rebalancer # unique name for the cluster
resources:
  - name: lock-group # unique resource name
    rebalancer: # we will provide our own rebalancer
      mode: USER_DEFINED
      class: domain.project.helix.rebalancer.UserDefinedRebalancerClass
...
```

### Example
We demonstrate plugging in a simple user-defined rebalancer as part of a revisit of the [distributed lock manager](./recipes/user_def_rebalancer.html) example. It includes a functional Rebalancer implementation, as well as the entire YAML file used to define the cluster.

Consider the case where partitions are locks in a lock manager and 6 locks are to be distributed evenly to a set of participants, and only one participant can hold each lock. We can define a rebalancing algorithm that simply takes the modulus of the lock number and the number of participants to evenly distribute the locks across participants. Helix allows capping the number of partitions a participant can accept, but since locks are lightweight, we do not need to define a restriction in this case. [This](https://git-wip-us.apache.org/repos/asf?p=helix.git;a=blob;f=recipes/user-defined-rebalancer/src/main/java/org/apache/helix/userdefinedrebalancer/LockManagerRebalancer.java;h=a232842d6ec1a8706c769bead458e29a658b4db5;hb=94e1079aa83daee27bfa9f1b1ca08367203e88d8) is a succinct implementation of this algorithm.

Here is the ResourceAssignment emitted by the user-defined rebalancer for a 3-participant system whenever there is a change to the set of participants.

* Participant_A joins

```
{
  "lock_0": { "Participant_A": "LOCKED"},
  "lock_1": { "Participant_A": "LOCKED"},
  "lock_2": { "Participant_A": "LOCKED"},
  "lock_3": { "Participant_A": "LOCKED"},
  "lock_4": { "Participant_A": "LOCKED"},
  "lock_5": { "Participant_A": "LOCKED"},
}
```

A ResourceAssignment is a mapping for each resource of partition to the participant serving each replica and the state of each replica. The state model is a simple LOCKED/RELEASED model, so participant A holds all lock partitions in the LOCKED state.

* Participant_B joins

```
{
  "lock_0": { "Participant_A": "LOCKED"},
  "lock_1": { "Participant_B": "LOCKED"},
  "lock_2": { "Participant_A": "LOCKED"},
  "lock_3": { "Participant_B": "LOCKED"},
  "lock_4": { "Participant_A": "LOCKED"},
  "lock_5": { "Participant_B": "LOCKED"},
}
```

Now that there are two participants, the simple mod-based function assigns every other lock to the second participant. On any system change, the rebalancer is invoked so that the application can define how to redistribute its resources.

* Participant_C joins (steady state)

```
{
  "lock_0": { "Participant_A": "LOCKED"},
  "lock_1": { "Participant_B": "LOCKED"},
  "lock_2": { "Participant_C": "LOCKED"},
  "lock_3": { "Participant_A": "LOCKED"},
  "lock_4": { "Participant_B": "LOCKED"},
  "lock_5": { "Participant_C": "LOCKED"},
}
```

This is the steady state of the system. Notice that four of the six locks now have a different owner. That is because of the naïve modulus-based assignmemt approach used by the user-defined rebalancer. However, the interface is flexible enough to allow you to employ consistent hashing or any other scheme if minimal movement is a system requirement.

* Participant_B fails

```
{
  "lock_0": { "Participant_A": "LOCKED"},
  "lock_1": { "Participant_C": "LOCKED"},
  "lock_2": { "Participant_A": "LOCKED"},
  "lock_3": { "Participant_C": "LOCKED"},
  "lock_4": { "Participant_A": "LOCKED"},
  "lock_5": { "Participant_C": "LOCKED"},
}
```

On any node failure, as in the case of node addition, the rebalancer is invoked automatically so that it can generate a new mapping as a response to the change. Helix ensures that the Rebalancer has the opportunity to reassign locks as required by the application.

* Participant_B (or the replacement for the original Participant_B) rejoins

```
{
  "lock_0": { "Participant_A": "LOCKED"},
  "lock_1": { "Participant_B": "LOCKED"},
  "lock_2": { "Participant_C": "LOCKED"},
  "lock_3": { "Participant_A": "LOCKED"},
  "lock_4": { "Participant_B": "LOCKED"},
  "lock_5": { "Participant_C": "LOCKED"},
}
```

The rebalancer was invoked once again and the resulting ResourceAssignment reflects the steady state.

### Caveats
- The rebalancer class must be available at runtime, or else Helix will not attempt to rebalance at all
