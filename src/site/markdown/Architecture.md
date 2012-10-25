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


Helix aims to provide the following abilities to a distributed system

* Auto management of a cluster hosting partitioned, replicated resources
* Soft and hard failure detection and handling.
* Automatic load balancing via smart placement of resources on servers(nodes) based on server capacity and resource profile (size of partition, access patterns, etc)
* Centralized config management and self discovery. Eliminates the need to modify config on each node.
* Fault tolerance and optimized rebalancing during cluster expansion.
* Manages entire operational lifecycle of a node. Addition, start, stop, enable/disable without downtime.
* Monitor cluster health and provide alerts on SLA violation.
* Service discovery mechanism to route requests.

To build such a system, we need a mechanism to co-ordinate between different nodes/components in the system. This mechanism can be achieved with a software that reacts to any change in the cluster and comes up with a set of tasks needed to bring the cluster to a stable state. The set of tasks will be assigned to one or more nodes in the cluster. Helix serves this purpose of managing the various components in the cluster.

![Helix Design](images/system.png)

Distributed System Components

In general any distributed system cluster will have the following

* Set of nodes also referred to as an instance.
* Set of resources which can be a database, lucene index or a task.
* Each resource is also partitioned into one or more Partitions. 
* Each partition may have one or more copies called replicas.
* Each replica can have a state associated with it. For example Master, Slave, Leader, Standby, Online, Offline etc

Roles
-----

Not all nodes in a distributed system will perform similar functionality. For e.g, a few nodes might be serving requests, few nodes might be sending the request and some nodes might be controlling the nodes in the cluster. Based on functionality we have grouped them into

We have divided Helix in 3 logical components based on their responsibility 

1. PARTICIPANT: The nodes that actually host the distributed resources.
2. SPECTATOR: The nodes that simply observe the PARTICIPANT State and route the request accordingly. Routers, for example, need to know the Instance on which a partition is hosted and its state in order to route the request to the appropriate end point.
3. CONTROLLER: The controller observes and controls the PARTICIPANT nodes. It is responsible for coordinating all transitions in the cluster and ensuring that state constraints are satisfied and cluster stability is maintained. 


These are simply logical components and can be deployed as per the system requirements. For example.

1. Controller can be deployed as a separate service. 
2. Controller can be deployed along with Participant but only one Controller will be active at any given time.

Both have pros and cons, which will be discussed later and one can chose the mode of deployment as per system needs.


## Cluster state/metadata store

We need a distributed store to maintain the state of the cluster and a notification system to notify if there is any change in the cluster state. Helix uses Zookeeper to achieve this functionality.

Zookeeper provides

* A way to represent PERSISTENT state which basically remains until its deleted.
* A way to represent TRANSIENT/EPHEMERAL state which vanishes when the process that created the STATE dies.
* Notification mechanism when there is a change in PERSISTENT/EPHEMERAL STATE

The name space provided by ZooKeeper is much like that of a standard file system. A name is a sequence of path elements separated by a slash (/). Every node[ZNODE] in ZooKeeper's name space is identified by a path.

More info on Zookeeper can be found here http://zookeeper.apache.org

## Statemachine and constraints

Even though the concept of Resource, Partition, Replicas is common to most distributed systems, one thing that differentiates one distributed system from another is the way each partition is assigned a state and the constraints on each state.

For example, 

1. If a system is serving READ ONLY data then all partitions replicas are equal and they can either be ONLINE or OFFLINE.
2. If a system takes BOTH READ and WRITES but ensure that WRITES go through only one partition then the states will be MASTER,SLAVE and OFFLINE. Writes go through the MASTER and is replicated to the SLAVES. Optionally, READS can go through SLAVES  

Apart from defining State for each partition, the transition path to each STATE can be application specific. For example, in order to become master it might be a requirement to first become a SLAVE. This ensures that if the SLAVE does not have the data as part of OFFLINE-SLAVE transition it can bootstrap data from other nodes in the system.

Helix provides a way to configure an application specific state machine along with constraints on each state. Along with constraints on State, Helix also provides a way to specify constraints on transitions.(More on this later)


<pre><code>
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

</code></pre>

![Helix Design](images/statemachine.png)

## Concepts

The following terminologies are used in Helix to model a state machine.

* IDEALSTATE:The state in which we need the cluster to be in if all nodes are up and running. In other words, all state constraints are satisfied.
* CURRENSTATE:Represents the current state of each node in the cluster 
* EXTERNALVIEW:Represents the combined view of CURRENTSTATE of all nodes.  

Goal of Helix is always to make the CURRENTSTATE of the system same as the IDEALSTATE. Some scenarios where this may not be true are

* When all nodes are down
* When one or more nodes fail
* New nodes are added and the partitions need to be reassigned.

### IDEALSTATE

Helix lets the application define the IdealState on a resource basis which basically consists of

* List of partitions. Example 64
* Number of replicas for each partition. Example 3
* Node and State for each replica.

Example

* Partition-1, replica-1, Master, Node-1
* Partition-1, replica-2, Slave, Node-2
* Partition-1, replica-3, Slave, Node-3
* .....
* .....
* Partition-p, replica-3, Slave, Node-n

Helix comes with various algorithms to automatically assign the partitions to nodes. The default algorithm minimizes the number of shuffles that happen when new nodes are added to the system

### CURRENTSTATE

Every Instance in the cluster hosts one or more partitions of a resource. Each of the partitions has a State associated with it.

Example Node-1

* Partition-1, Master
* Partition-2, Slave
* ....
* ....
* Partition-p, Slave

### EXTERNALVIEW

External clients needs to know the state of each partition in the cluster and the Node hosting that partition. Helix provides one view of the system to SPECTATORS as EXTERNAL VIEW. EXTERNAL VIEW is simply an aggregate of all CURRENTSTATE

* Partition-1, replica-1, Master, Node-1
* Partition-1, replica-2, Slave, Node-2
* Partition-1, replica-3, Slave, Node-3
* .....
* .....
* Partition-p, replica-3, Slave, Node-n

## Process Workflow

Mode of operation in a cluster

A node process can be one of the following

* PARTICIPANT: The process registers itself in the cluster and acts on the messages received in its queue and updates the current state.  Example:Storage Node
* SPECTATOR: The process is simply interested in the changes in the externalView. Router is a spectator of Storage cluster.
* CONTROLLER:This process actively controls the cluster by reacting to changes in Cluster State and sending messages to PARTICIPANTS


### Participant Node Process


* When Node starts up, it registers itself under LIVEINSTANCES
* After registering, it waits for new Messages in the message queue
* When it receives a message, it will perform the required task as indicated in the message.
* After the task is completed, depending on the task outcome it updates the CURRENTSTATE.

### Controller Process

* Watches IDEALSTATE
* Node goes down/comes up or Node is added/removed. Watches LIVEINSTANCES and CURRENTSTATE of each Node in the cluster
* Triggers appropriate state transition by sending message to PARTICIPANT

### Spectator Process

* When the process starts it asks cluster manager agent to be notified of changes in ExternalView
* When ever it receives a notification it reads the externalView and performs required duties. For Router, it updates its routing table.

h4. Interaction between controller, participant and spectator

The following picture shows how controllers, participants and spectators interact with each other.

![Helix Architecture](images/helix-architecture.png)

## Core algorithm


* Controller gets the Ideal State and the Current State of active storage nodes from ZK
* Compute the delta between Ideal State and Current State for each partition across all participant nodes
* For each partition compute tasks based on State Machine Table. Its possible to configure priority on the state Transition. For example in case of Master Slave:
    * Attempt Mastership transfer if possible without violating constraint.
    * Partition Addition
    * Drop Partition 
* Add the tasks in parallel if possible to respective queue for each storage node keeping in mind
* The tasks added are mutually independent.
* If a task is dependent on another task being completed do not add that task.
* After any task is completed by Participant, Controllers gets notified of the change and State Transition algorithm is re-run until the current state is same as Ideal State.

## Helix znode layout
Helix organizes znodes under clusterName in multiple levels. 
The top level (under clusterName) znodes are all Helix defined and in upper case
* PROPERTYSTORE: application property store
* STATEMODELDEFES: state model definitions
* INSTANCES: instance runtime information including current state and messages
* CONFIGS: configurations
* IDEALSTATES: ideal states
* EXTERNALVIEW: external views
* LIVEINSTANCES: live instances
* CONTROLLER: cluster controller runtime information

Under INSTANCES, there are runtime znodes for each instance. An instance organizes znodes as follows:
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
