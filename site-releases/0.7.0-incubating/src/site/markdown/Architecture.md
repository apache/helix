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
  <title>Architecture</title>
</head>

Architecture
----------------------------
Helix aims to provide the following abilities to a distributed system:

* Automatic management of a cluster hosting partitioned, replicated resources.
* Soft and hard failure detection and handling.
* Automatic load balancing via smart placement of resources on servers(nodes) based on server capacity and resource profile (size of partition, access patterns, etc).
* Centralized config management and self discovery. Eliminates the need to modify config on each node.
* Fault tolerance and optimized rebalancing during cluster expansion.
* Manages entire operational lifecycle of a node. Addition, start, stop, enable/disable without downtime.
* Monitor cluster health and provide alerts on SLA violation.
* Service discovery mechanism to route requests.

To build such a system, we need a mechanism to co-ordinate between different nodes and other components in the system. This mechanism can be achieved with software that reacts to any change in the cluster and comes up with a set of tasks needed to bring the cluster to a stable state. The set of tasks will be assigned to one or more nodes in the cluster. Helix serves this purpose of managing the various components in the cluster.

![Helix Design](images/system.png)

Distributed System Components

In general any distributed system cluster will have the following components and properties:

* A set of nodes also referred to as instances.
* A set of resources which can be databases, lucene indexes or tasks.
* Each resource is also partitioned into one or more Partitions. 
* Each partition may have one or more copies called replicas.
* Each replica can have a state associated with it. For example Master, Slave, Leader, Standby, Online, Offline etc

Roles
-----

![Helix Design](images/HELIX-components.png)

Not all nodes in a distributed system will perform similar functionalities. For example, a few nodes might be serving requests and a few nodes might be sending requests, and some nodes might be controlling the nodes in the cluster. Thus, Helix categorizes nodes by their specific roles in the system.

We have divided Helix nodes into 3 logical components based on their responsibilities:

1. Participant: The nodes that actually host the distributed resources.
2. Spectator: The nodes that simply observe the Participant state and route the request accordingly. Routers, for example, need to know the instance on which a partition is hosted and its state in order to route the request to the appropriate end point.
3. Controller: The controller observes and controls the Participant nodes. It is responsible for coordinating all transitions in the cluster and ensuring that state constraints are satisfied and cluster stability is maintained. 

These are simply logical components and can be deployed as per the system requirements. For example:

1. The controller can be deployed as a separate service
2. The controller can be deployed along with a Participant but only one Controller will be active at any given time.

Both have pros and cons, which will be discussed later and one can chose the mode of deployment as per system needs.


## Cluster state metadata store

We need a distributed store to maintain the state of the cluster and a notification system to notify if there is any change in the cluster state. Helix uses Zookeeper to achieve this functionality.

Zookeeper provides:

* A way to represent PERSISTENT state which basically remains until its deleted.
* A way to represent TRANSIENT/EPHEMERAL state which vanishes when the process that created the state dies.
* Notification mechanism when there is a change in PERSISTENT and EPHEMERAL state

The namespace provided by ZooKeeper is much like that of a standard file system. A name is a sequence of path elements separated by a slash (/). Every node[ZNode] in ZooKeeper\'s namespace is identified by a path.

More info on Zookeeper can be found at http://zookeeper.apache.org

## State machine and constraints

Even though the concepts of Resources, Partitions, and Replicas are common to most distributed systems, one thing that differentiates one distributed system from another is the way each partition is assigned a state and the constraints on each state.

For example:

1. If a system is serving read-only data then all partition\'s replicas are equal and they can either be ONLINE or OFFLINE.
2. If a system takes _both_ reads and writes but ensure that writes go through only one partition, the states will be MASTER, SLAVE, and OFFLINE. Writes go through the MASTER and replicate to the SLAVEs. Optionally, reads can go through SLAVES.

Apart from defining state for each partition, the transition path to each state can be application specific. For example, in order to become MASTER it might be a requirement to first become a SLAVE. This ensures that if the SLAVE does not have the data as part of OFFLINE-SLAVE transition it can bootstrap data from other nodes in the system.

Helix provides a way to configure an application specific state machine along with constraints on each state. Along with constraints on STATE, Helix also provides a way to specify constraints on transitions.  (More on this later.)

```
          OFFLINE  | SLAVE  |  MASTER  
         _____________________________
        |          |        |         |
OFFLINE |   N/A    | SLAVE  | SLAVE   |
        |__________|________|_________|
        |          |        |         |
SLAVE   |  OFFLINE |   N/A  | MASTER  |
        |__________|________|_________|
        |          |        |         |
MASTER  | SLAVE    | SLAVE  |   N/A   |
        |__________|________|_________|

```

![Helix Design](images/statemachine.png)

## Concepts

The following terminologies are used in Helix to model a state machine.

* IdealState: The state in which we need the cluster to be in if all nodes are up and running. In other words, all state constraints are satisfied.
* CurrentState: Represents the actual current state of each node in the cluster 
* ExternalView: Represents the combined view of CurrentState of all nodes.  

The goal of Helix is always to make the CurrentState of the system same as the IdealState. Some scenarios where this may not be true are:

* When all nodes are down
* When one or more nodes fail
* New nodes are added and the partitions need to be reassigned

### IdealState

Helix lets the application define the IdealState on a resource basis which basically consists of:

* List of partitions. Example: 64
* Number of replicas for each partition. Example: 3
* Node and State for each replica.

Example:

* Partition-1, replica-1, Master, Node-1
* Partition-1, replica-2, Slave, Node-2
* Partition-1, replica-3, Slave, Node-3
* .....
* .....
* Partition-p, replica-3, Slave, Node-n

Helix comes with various algorithms to automatically assign the partitions to nodes. The default algorithm minimizes the number of shuffles that happen when new nodes are added to the system.

### CurrentState

Every instance in the cluster hosts one or more partitions of a resource. Each of the partitions has a state associated with it.

Example Node-1

* Partition-1, Master
* Partition-2, Slave
* ....
* ....
* Partition-p, Slave

### ExternalView

External clients needs to know the state of each partition in the cluster and the Node hosting that partition. Helix provides one view of the system to Spectators as _ExternalView_. ExternalView is simply an aggregate of all node CurrentStates.

* Partition-1, replica-1, Master, Node-1
* Partition-1, replica-2, Slave, Node-2
* Partition-1, replica-3, Slave, Node-3
* .....
* .....
* Partition-p, replica-3, Slave, Node-n

## Process Workflow

Mode of operation in a cluster

A node process can be one of the following:

* Participant: The process registers itself in the cluster and acts on the messages received in its queue and updates the current state.  Example: a storage node in a distributed database
* Spectator: The process is simply interested in the changes in the Externalview.
* Controller: This process actively controls the cluster by reacting to changes in cluster state and sending messages to Participants.


### Participant Node Process

* When Node starts up, it registers itself under _LiveInstances_
* After registering, it waits for new _Messages_ in the message queue
* When it receives a message, it will perform the required task as indicated in the message
* After the task is completed, depending on the task outcome it updates the CurrentState

### Controller Process

* Watches IdealState
* Notified when a node goes down/comes up or node is added/removed. Watches LiveInstances and CurrentState of each node in the cluster
* Triggers appropriate state transitions by sending message to Participants

### Spectator Process

* When the process starts, it asks the Helix agent to be notified of changes in ExternalView
* Whenever it receives a notification, it reads the Externalview and performs required duties.

#### Interaction between controller, participant and spectator

The following picture shows how controllers, participants and spectators interact with each other.

![Helix Architecture](images/helix-architecture.png)

## Core algorithm

* Controller gets the IdealState and the CurrentState of active storage nodes from Zookeeper
* Compute the delta between IdealState and CurrentState for each partition across all participant nodes
* For each partition compute tasks based on the State Machine Table. It\'s possible to configure priority on the state Transition. For example, in case of Master-Slave:
    * Attempt mastership transfer if possible without violating constraint.
    * Partition Addition
    * Drop Partition 
* Add the tasks in parallel if possible to the respective queue for each storage node (if the tasks added are mutually independent)
* If a task is dependent on another task being completed, do not add that task
* After any task is completed by a Participant, Controllers gets notified of the change and the State Transition algorithm is re-run until the CurrentState is same as IdealState.

## Helix ZNode layout

Helix organizes znodes under clusterName in multiple levels. 

The top level (under the cluster name) ZNodes are all Helix-defined and in upper case:

* PROPERTYSTORE: application property store
* STATEMODELDEFES: state model definitions
* INSTANCES: instance runtime information including current state and messages
* CONFIGS: configurations
* IDEALSTATES: ideal states
* EXTERNALVIEW: external views
* LIVEINSTANCES: live instances
* CONTROLLER: cluster controller runtime information

Under INSTANCES, there are runtime ZNodes for each instance. An instance organizes ZNodes as follows:

* CURRENTSTATES
    * sessionId
    * resourceName
* ERRORS
* STATUSUPDATES
* MESSAGES
* HEALTHREPORT

Under CONFIGS, there are different scopes of configurations:

* RESOURCE: contains resource scope configurations
* CLUSTER: contains cluster scope configurations
* PARTICIPANT: contains participant scope configurations

The following image shows an example of Helix znodes layout for a cluster named "test-cluster":

![Helix znode layout](images/helix-znode-layout.png)
