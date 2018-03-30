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
  <title>Tutorial - State Machine Configuration</title>
</head>

## [Helix Tutorial](./Tutorial.html): State Machine Configuration

In this chapter, we\'ll learn about the state models provided by Helix, and how to create your own custom state model.

### State Models

Helix comes with 3 default state models that are commonly used.  It is possible to have multiple state models in a cluster.
Every resource that is added should be configured to use a state model that govern its _ideal state_.

#### MASTER-SLAVE

* 3 states: OFFLINE, SLAVE, MASTER
* Maximum number of masters: 1
* Slaves are based on the replication factor. The replication factor can be specified while adding the resource.


#### ONLINE-OFFLINE

* Has 2 states: OFFLINE and ONLINE.  This simple state model is a good starting point for most applications.

#### LEADER-STANDBY

* 1 Leader and multiple stand-bys.  The idea is that exactly one leader accomplishes a designated task, the stand-bys are ready to take over if the leader fails.

### Constraints

In addition to the state machine configuration, one can specify the constraints of states and transitions.

For example, one can say:

* MASTER:1
<br/>Maximum number of replicas in MASTER state at any time is 1

* OFFLINE-SLAVE:5
<br/>Maximum number of OFFLINE-SLAVE transitions that can happen concurrently in the system is 5 in this example.

#### Dynamic State Constraints

We also support two dynamic upper bounds for the number of replicas in each state:

* N: The number of replicas in the state is at most the number of live participants in the cluster
* R: The number of replicas in the state is at most the specified replica count for the partition

#### State Priority

Helix uses a greedy approach to satisfy the state constraints. For example, if the state machine configuration says it needs 1 MASTER and 2 SLAVES, but only 1 node is active, Helix must promote it to MASTER. This behavior is achieved by providing the state priority list as \[MASTER, SLAVE\].

#### State Transition Priority

Helix tries to fire as many transitions as possible in parallel to reach the stable state without violating constraints. By default, Helix simply sorts the transitions alphabetically and fires as many as it can without violating the constraints. You can control this by overriding the priority order.

### Special States

There are a few Helix-defined states that are important to be aware of.

#### DROPPED

The DROPPED state is used to signify a replica that was served by a given participant, but is no longer served. This allows Helix and its participants to effectively clean up. There are two requirements that every new state model should follow with respect to the DROPPED state:

* The DROPPED state must be defined
* There must be a path to DROPPED for every state in the model

#### ERROR

The ERROR state is used whenever the participant serving a partition encountered an error and cannot continue to serve the partition. HelixAdmin has \"reset\" functionality to allow for participants to recover from the ERROR state.

### Annotated Example

Below is a complete definition of a Master-Slave state model. Notice the fields marked REQUIRED; these are essential for any state model definition.

```
StateModelDefinition stateModel = new StateModelDefinition.Builder("MasterSlave")
  // OFFLINE is the state that the system starts in (initial state is REQUIRED)
  .initialState("OFFLINE")

  // Lowest number here indicates highest priority, no value indicates lowest priority
  .addState("MASTER", 1)
  .addState("SLAVE", 2)
  .addState("OFFLINE")

  // Note the special inclusion of the DROPPED state (REQUIRED)
  .addState(HelixDefinedState.DROPPED.toString())

  // No more than one master allowed
  .upperBound("MASTER", 1)

  // R indicates an upper bound of number of replicas for each partition
  .dynamicUpperBound("SLAVE", "R")

  // Add some high-priority transitions
  .addTransition("SLAVE", "MASTER", 1)
  .addTransition("OFFLINE", "SLAVE", 2)

  // Using the same priority value indicates that these transitions can fire in any order
  .addTransition("MASTER", "SLAVE", 3)
  .addTransition("SLAVE", "OFFLINE", 3)

  // Not specifying a value defaults to lowest priority
  // Notice the inclusion of the OFFLINE to DROPPED transition
  // Since every state has a path to OFFLINE, they each now have a path to DROPPED (REQUIRED)
  .addTransition("OFFLINE", HelixDefinedState.DROPPED.toString())

  // Create the StateModelDefinition instance
  .build();

  // Use the isValid() function to make sure the StateModelDefinition will work without issues
  Assert.assertTrue(stateModel.isValid());
```
