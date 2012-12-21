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

Apache Incubator
---------------
Helix is now part of Apache Incubator. 

Project page: http://incubator.apache.org/projects/helix.html

Subscribe to mailing list by sending an email to user-subscribe@helix.incubator.apache.org 


WHAT IS HELIX
--------------
Helix is a generic cluster management framework used for automatic management of partitioned, replicated and distributed resources hosted on a cluster of nodes. Helix provides the following features: 

1. Automatic assignment of resource/partition to nodes
2. Node failure detection and recovery
3. Dynamic addition of Resources 
4. Dynamic addition of nodes to the cluster
5. Pluggable distributed state machine to manage the state of a resource via state transitions
6. Automatic load balancing and throttling of transitions 

-----

OVERVIEW
-------------
Helix uses terms that are commonly used to describe distributed data system concepts. 

1. Cluster: A logical set of Instances that perform a similar set of activities. 
2. Instance: An Instance is a logical entity in the cluster that can be identified by a unique Id. 
3. Node: A Node is a physical entity in the cluster. A Node can have one or more logical Instances. 
4. Resource: A resource represents the logical entity hosted by the distributed system. It can be a database name, index or a task group name 
5. Partition: A resource is generally split into one or more partitions.
6. Replica: Each partition can have one or more replicas
7. State: Each replica can have state associated with it. For example: Master, Slave, Leader, Stand By, Offline, Online etc. 

To summarize, a resource (database, index or any task) in general is partitioned, replicated and distributed among the Instance/nodes in the cluster and each partition has a state associated with it. 

Helix manages the state of a resource by supporting a pluggable distributed state machine. One can define the state machine table along with the constraints for each state. 

Here are some common state models used

1. Master, Slave
2. Online, Offline
3. Leader, Standby.

For example in the case of a MasterSlave state model one can specify the state machine as follows. The table says given a start state and an end state what should be the next state. 
For example, if the current state is Offline and the target state is Master, the table says that the next state is Slave.  So in this case, Helix issues an Offline-Slave transition

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

Helix also supports the ability to provide constraints on each state. For example in a MasterSlave state model with a replication factor of 3 one can say 

    MASTER:1 
    SLAVE:2

Helix will automatically maintain 1 Master and 2 Slaves by initiating appropriate state transitions on each instance in the cluster. 

Each transition results in a partition moving from its CURRENT state to a NEW state. These transitions are triggered on changes in the cluster state like 

* Node start up
* Node soft and hard failures 
* Addition of resources
* Addition of nodes

In simple words, Helix is a distributed state machine with support for constraints on each state.

Helix framework can be used to build distributed, scalable, elastic and fault tolerant systems by configuring the distributed state machine and its constraints based on application requirements. The application has to provide the implementation for handling state transitions appropriately. Example 

Once the state machine and constraints are configured through Helix, application will have the provide implementation to handle the transitions appropriately.  

<pre><code>
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
</code></pre>

Once the state machine is configured, the framework allows one to 

* Dynamically add nodes to the cluster
* Automatically modify the topology(rebalance partitions) of the cluster  
* Dynamically add resources to the cluster
* Enable/disable partition/instances for software upgrade without impacting availability.

Helix uses Zookeeper for maintaining the cluster state and change notifications.

WHY HELIX
-------------
Helix approach of using a distributed state machine with constraints on state and transitions has benefited us in multiple ways.

* Abstract cluster management aspects from the core functionality of DDS.
* Each node in DDS is not aware of the global state since they simply have to follow . This proved quite useful since we could deploy the same system in different topologies.
* Since the controller's goal is to satisfy state machine constraints at all times, use cases like cluster startup, node failure, cluster expansion are solved in a similar way.

At LinkedIn, we have been able to use this to manage 3 different distributed systems that look very different on paper.  

----------------

Additional Info
---------------

Documentation: [Home](https://github.com/linkedin/helix/wiki/Home)  
Sample App using Helix: [rabbitmq-consumer-group] (https://github.com/linkedin/helix/wiki/Sample_App)  
Quickstart guide: [How to build and run a mock example](https://github.com/linkedin/helix/wiki/Quickstart)  
Architecture: [Helix Architecture](https://github.com/linkedin/helix/wiki/Architecture)  
Features: [Helix Features](https://github.com/linkedin/helix/wiki/Features)  
ApiUsage: http://linkedin.github.com/helix/apidocs/
UseCases: [Helix LinkedIn Usecases](https://github.com/linkedin/helix/wiki/UseCases)  

Build Informations
------------------
To deploy web site to Apache infrastructure: sh ./deploySite.sh -Dusername=uid -Dpassword=pwd (-DskipTests if you don't want to run units tests)
uid is your asf id, pwd is the password


   
