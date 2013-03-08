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


Pages
---------------
* [Quickstart](./Quickstart.html)
* [Architecture](./Architecture.html)
* [Features](./Features.html)
* [ApiUsage](./ApiUsage.html)
* [Javadocs](./apidocs/index.html)
* [UseCases](./UseCases.html)
* Recipes
    - [Distributed lock manager](./recipes/lock_manager.html)
    - [Rabbit MQ consumer group](./recipes/rabbitmq_consumer_group.html)
    - [Rsync replicated file store](./recipes/rsync_replicated_file_store.html)

WHAT IS HELIX
--------------
Helix is a generic cluster management framework used for the automatic management of partitioned, replicated and distributed resources hosted on a cluster of nodes. Helix provides the following features: 

1. Automatic assignment of resource/partition to nodes
2. Node failure detection and recovery
3. Dynamic addition of Resources 
4. Dynamic addition of nodes to the cluster
5. Pluggable distributed state machine to manage the state of a resource via state transitions
6. Automatic load balancing and throttling of transitions 

-----

OVERVIEW
---------

A distributed system comprises of one or more *nodes*. Depending on the purpose, each node performs a specific task. For example, in a search system it can be a index, in a pub sub system it can be a topic/queue, in storage system it can be a database. Helix refers to such tasks as a *resource*. In order to scale the system, each node is responsible for a part of task referred to as *partition*. For scalability and fault tolerance, task associated with each partition can run on multiple nodes. Helix refers to them as *replica*. 
 
Helix refers to each of the node in the cluster as a *PARTICIPANT*. As seen in many distributed system, there is a central component called *CONTROLLER* that co-ordinates the *PARTICIPANT*'s  during start up, failures and cluster expansion. In most distributed systems need to provide a service discovery mechanism for external entities like clients, request routers, load balancers to interact with the distributed system. These external entities are referred as SPECTATOR.

Helix is built on top of Zookeeper and uses it store the cluster state and serves as the communication channel between CONTROLLER, PARTICIPANT and spectator. There is no single point of failure in Helix.

Helix managed distributed system architecture.

![Helix Design](images/HELIX-components.png)


WHAT MAKES IT GENERIC
---------------------

Even though most distributed systems follow similar mechanism of co-ordinating the nodes through a controller or zookeeper, the implementation is 
specific to the use case. Helix abstracts out the cluster management of distributed system from its core functionality. 

Helix allows one to express the system behavior via  

#### STATE MACHINE
State machine allows one to express the different roles a replica can take up and transition from one role to another.

* Set of valid states (S1,S2,S3 etc) for each replica
* Set of valid transitions that allow replicas to transition from one state to another. 
   
#### CONSTRAINTS
Helix allows one to specify constraints on states and transitions. 

* Minimum and maximum number of replicas that need to be in a given state. For example S3: Max=1 S2: Min=2, Max=3
* Set a max concurrency limit of each transition type. For example, if S1-S2 involves moving data, one can limit the data movement by setting limit on maximum number of concurrent (S1->S2) transitions per node to 5.  

#### OBJECTIVES
Objectives are used to control the replica placement strategy across the nodes. For example

* Replicas must be evenly distributed across nodes.  
* Replicas of one partition must be on different nodes/racks.
* When a node fails, its load must be evenly distributed among rest of the nodes.
* When new nodes are added, it must result in minimum number of movements.    

EXAMPLE
-------

Consider the simple use cases where all partitions are actively processing search query request. 
We can express it using a OnlineOffline state model where a task can be either 
ONLINE (task is active) or OFFLINE (not active).

Similarly take a slightly more complicated system, where we need three states OFFLINE, SLAVE and MASTER. 

The following state machine table provides transition from start state to End state. For example, if the current state is Offline and the target state is Master,
 the table says that the first transition must be Offline-Slave and then Slave-Master.

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


Another unique feature of Helix is it allows one to add constraints on each state and transitions. 

For example 
In a OnlineOffline state model one can enforce a constraint that there should be 3 replicas in ONLINE state per partition.

    ONLINE:3

In a MasterSlave state model with a replication factor of 3 one can enforce a single master by specifying constraints on number of Masters and Slaves.

    MASTER:1 
    SLAVE:2

Given these constraints, Helix will ensure that there is 1 Master and 2 Slaves by initiating appropriate state transitions in the cluster.


Apart from Constraints on STATES, Helix supports constraints on transitions as well. For example, consider a OFFLINE-BOOTSTRAP transition where a service download the index over the network. 
Without any throttling during start up of a cluster, all nodes might start downloading at once which might impact the system stability. 
Using Helix with out changing any application code, one can simply place a constraint of max 5 transitions OFFLINE-BOOTSTRAP across the entire cluster.

The constraints can be at any scope node, resource, transition type and 

Helix comes with 3 commonly used state models, you can also plugin your custom state model. 

1. Master, Slave
2. Online, Offline
3. Leader, Standby.


Helix framework can be used to build distributed, scalable, elastic and fault tolerant systems by configuring the distributed state machine and its constraints based on application requirements. The application has to provide the implementation for handling state transitions appropriately. Example 


```
MasterSlaveStateModel extends HelixStateModel {

  void onOfflineToSlave(Message m, NotificationContext context){
    print("Transitioning from Offline to Slave for resource:"+ m.getResourceName() + " and partition:"+ m.getPartitionName());
  }
  void onSlaveToMaster(Message m, NotificationContext context){
    print("Transitioning from Slave to Master for resource:"+ m.getResourceName() + " and partition:"+ m.getPartitionName());
  }
  void onMasterToSlave(Message m, NotificationContext context){
    print("Transitioning from Master to Slave for resource:"+ m.getResourceName() + " and partition:"+ m.getPartitionName());
  }
  void onSlaveToOffline(Message m, NotificationContext context){
    print("Transitioning from Slave to Offline for resource:"+ m.getResourceName() + " and partition:"+ m.getPartitionName());
  }
}
```

Each transition results in a partition moving from its CURRENT state to a NEW state. These transitions are triggered on changes in the cluster state like 

* Node start up
* Node soft and hard failures 
* Addition of resources
* Addition of nodes


TERMINOLOGIES
-------------
Helix uses terms that are commonly used to describe distributed data system concepts. 

1. Cluster: A logical set of Instances that perform a similar set of activities. 
2. Instance: An Instance is a logical entity in the cluster that can be identified by a unique Id. 
3. Node: A Node is a physical entity in the cluster. A Node can have one or more logical Instances. 
4. Resource: A resource represents the logical entity hosted by the distributed system. It can be a database name, index or a task group name 
5. Partition: A resource is generally split into one or more partitions.
6. Replica: Each partition can have one or more replicas
7. State: Each replica can have state associated with it. For example: Master, Slave, Leader, Stand By, Offline, Online etc. 



WHY HELIX
-------------
Helix approach of using a distributed state machine with constraints on state and transitions has the following benefits

* Abstract cluster management from the core functionality.
* Quick transformation from a single node system to a distributed system.
* PARTICIPANT is not aware of the global state since they simply have to follow the instructions issued by the CONTROLLER. This design provide clear division of responsibilities and easier to debug issues.
* Since the controller's goal is to satisfy state machine constraints at all times, use cases like cluster startup, node failure, cluster expansion are solved in a similar way.


BUILD INSTRUCTIONS
-------------------------

Requirements: Jdk 1.6+, Maven 2.0.8+

```
    git clone https://git-wip-us.apache.org/repos/asf/incubator-helix.git
    cd incubator-helix
    mvn install package -DskipTests 
```

Maven dependency

```
    <dependency>
      <groupId>org.apache.helix</groupId>
      <artifactId>helix-core</artifactId>
      <version>0.6.0-incubating</version>
    </dependency>
```

[Download](./download.html) Helix artifacts from here.
   
PUBLICATIONS
-------------

* Untangling cluster management using Helix at [SOCC Oct 2012](http://www.socc2012.org/home/program)  
    - [paper](https://915bbc94-a-62cb3a1a-s-sites.googlegroups.com/site/acm2012socc/helix_onecol.pdf)
    - [presentation](http://www.slideshare.net/KishoreGopalakrishna/helix-socc-v10final)
* Building distributed systems using Helix Apache Con Feb 2013
    - [presentation at ApacheCon](http://www.slideshare.net/KishoreGopalakrishna/apache-con-buildingddsusinghelix)
    - [presentation at vmware](http://www.slideshare.net/KishoreGopalakrishna/apache-helix-presentation-at-vmware)

