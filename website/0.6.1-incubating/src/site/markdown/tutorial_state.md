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

Helix Tutorial: State Machine Configuration
-------------------------------------------

In this chapter, we\'ll learn about the state models provided by Helix, and how to create your own custom state model.

### State Models

Helix comes with 3 default state models that are commonly used.  It is possible to have multiple state models in a cluster.
Every resource that is added should be configured to use a single state model that will govern its _ideal state_.

#### MASTER-SLAVE

* Has 3 states: OFFLINE, SLAVE, MASTER
* Maximum # of masters: 1
* Slaves are based on the replication factor. The replication factor can be specified while adding the resource

#### ONLINE-OFFLINE

Has 2 states: OFFLINE and ONLINE.  This simple state model is a good starting point for most applications.

#### LEADER-STANDBY

1 Leader and multiple stand-bys.  The idea is that exactly one leader accomplishes a designated task, the stand-bys are ready to take over if the leader fails.

### Constraints

In addition to the state machine configuration, one can specify the constraints of states and transitions.

For example, one can say:

* MASTER:1

to indicate that the maximum number of replicas in MASTER state at any time is 1

* OFFLINE-SLAVE:5

to indicate that the maximum number of OFFLINE-SLAVE transitions that can happen concurrently in the system is 5 in this example.

#### State Priority

Helix uses a greedy approach to satisfy the state constraints. For example, if the state machine configuration says it needs 1 MASTER and 2 SLAVES, but only 1 node is active, Helix must promote it to MASTER. This behavior is achieved by providing the state priority list as MASTER,SLAVE.

#### State Transition Priority

Helix tries to fire as many transitions as possible in parallel to reach the stable state without violating constraints. By default, Helix simply sorts the transitions alphabetically and fires as many as it can without violating the constraints. You can control this by overriding the priority order.

