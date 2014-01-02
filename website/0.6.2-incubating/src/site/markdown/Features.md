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
  <title>Features</title>
</head>

Features
----------------------------


### CONFIGURING IDEALSTATE


Read concepts page for definition of Idealstate.

The placement of partitions in a DDS is very critical for reliability and scalability of the system. 
For example, when a node fails, it is important that the partitions hosted on that node are reallocated evenly among the remaining nodes. Consistent hashing is one such algorithm that can guarantee this.
Helix by default comes with a variant of consistent hashing based of the RUSH algorithm. 

This means given a number of partitions, replicas and number of nodes Helix does the automatic assignment of partition to nodes such that

* Each node has the same number of partitions and replicas of the same partition do not stay on the same node.
* When a node fails, the partitions will be equally distributed among the remaining nodes
* When new nodes are added, the number of partitions moved will be minimized along with satisfying the above two criteria.


Helix provides multiple ways to control the placement and state of a replica. 

```

            |AUTO REBALANCE|   AUTO     |   CUSTOM  |       
            -----------------------------------------
   LOCATION | HELIX        |  APP       |  APP      |
            -----------------------------------------
      STATE | HELIX        |  HELIX     |  APP      |
            -----------------------------------------
```

#### HELIX EXECUTION MODE 


Idealstate is defined as the state of the DDS when all nodes are up and running and healthy. 
Helix uses this as the target state of the system and computes the appropriate transitions needed in the system to bring it to a stable state. 

Helix supports 3 different execution modes which allows application to explicitly control the placement and state of the replica.

##### AUTO_REBALANCE

When the idealstate mode is set to AUTO_REBALANCE, Helix controls both the location of the replica along with the state. This option is useful for applications where creation of a replica is not expensive. Example

```
{
  "id" : "MyResource",
  "simpleFields" : {
    "IDEAL_STATE_MODE" : "AUTO_REBALANCE",
    "NUM_PARTITIONS" : "3",
    "REPLICAS" : "2",
    "STATE_MODEL_DEF_REF" : "MasterSlave",
  }
  "listFields" : {
    "MyResource_0" : [],
    "MyResource_1" : [],
    "MyResource_2" : []
  },
  "mapFields" : {
  }
}
```

If there are 3 nodes in the cluster, then Helix will internally compute the ideal state as 

```
{
  "id" : "MyResource",
  "simpleFields" : {
    "NUM_PARTITIONS" : "3",
    "REPLICAS" : "2",
    "STATE_MODEL_DEF_REF" : "MasterSlave",
  },
  "mapFields" : {
    "MyResource_0" : {
      "N1" : "MASTER",
      "N2" : "SLAVE",
    },
    "MyResource_1" : {
      "N2" : "MASTER",
      "N3" : "SLAVE",
    },
    "MyResource_2" : {
      "N3" : "MASTER",
      "N1" : "SLAVE",
    }
  }
}
```

Another typical example is evenly distributing a group of tasks among the currently alive processes. For example, if there are 60 tasks and 4 nodes, Helix assigns 15 tasks to each node. 
When one node fails Helix redistributes its 15 tasks to the remaining 3 nodes. Similarly, if a node is added, Helix re-allocates 3 tasks from each of the 4 nodes to the 5th node. 

#### AUTO

When the idealstate mode is set to AUTO, Helix only controls STATE of the replicas where as the location of the partition is controlled by application. Example: The below idealstate indicates thats 'MyResource_0' must be only on node1 and node2.  But gives the control of assigning the STATE to Helix.

```
{
  "id" : "MyResource",
  "simpleFields" : {
    "IDEAL_STATE_MODE" : "AUTO",
    "NUM_PARTITIONS" : "3",
    "REPLICAS" : "2",
    "STATE_MODEL_DEF_REF" : "MasterSlave",
  }
  "listFields" : {
    "MyResource_0" : [node1, node2],
    "MyResource_1" : [node2, node3],
    "MyResource_2" : [node3, node1]
  },
  "mapFields" : {
  }
}
```
In this mode when node1 fails, unlike in AUTO-REBALANCE mode the partition is not moved from node1 to others nodes in the cluster. Instead, Helix will decide to change the state of MyResource_0 in N2 based on the system constraints. For example, if a system constraint specified that there should be 1 Master and if the Master failed, then node2 will be made the new master. 

#### CUSTOM

Helix offers a third mode called CUSTOM, in which application can completely control the placement and state of each replica. Applications will have to implement an interface that Helix will invoke when the cluster state changes. 
Within this callback, the application can recompute the idealstate. Helix will then issue appropriate transitions such that Idealstate and Currentstate converges.

```
{
  "id" : "MyResource",
  "simpleFields" : {
      "IDEAL_STATE_MODE" : "CUSTOM",
    "NUM_PARTITIONS" : "3",
    "REPLICAS" : "2",
    "STATE_MODEL_DEF_REF" : "MasterSlave",
  },
  "mapFields" : {
    "MyResource_0" : {
      "N1" : "MASTER",
      "N2" : "SLAVE",
    },
    "MyResource_1" : {
      "N2" : "MASTER",
      "N3" : "SLAVE",
    },
    "MyResource_2" : {
      "N3" : "MASTER",
      "N1" : "SLAVE",
    }
  }
}
```

For example, the current state of the system might be 'MyResource_0' -> {N1:MASTER,N2:SLAVE} and the application changes the ideal state to 'MyResource_0' -> {N1:SLAVE,N2:MASTER}. Helix will not blindly issue MASTER-->SLAVE to N1 and SLAVE-->MASTER to N2 in parallel since it might result in a transient state where both N1 and N2 are masters.
Helix will first issue MASTER-->SLAVE to N1 and after its completed it will issue SLAVE-->MASTER to N2. 
 

### State Machine Configuration

Helix comes with 3 default state models that are most commonly used. Its possible to have multiple state models in a cluster. 
Every resource that is added should have a reference to the state model. 

* MASTER-SLAVE: Has 3 states OFFLINE,SLAVE,MASTER. Max masters is 1. Slaves will be based on the replication factor. Replication factor can be specified while adding the resource
* ONLINE-OFFLINE: Has 2 states OFFLINE and ONLINE. Very simple state model and most applications start off with this state model.
* LEADER-STANDBY:1 Leader and many stand bys. In general the standby's are idle.

Apart from providing the state machine configuration, one can specify the constraints of states and transitions.

For example one can say
Master:1. Max number of replicas in Master state at any time is 1.
OFFLINE-SLAVE:5 Max number of Offline-Slave transitions that can happen concurrently in the system

STATE PRIORITY
Helix uses greedy approach to satisfy the state constraints. For example if the state machine configuration says it needs 1 master and 2 slaves but only 1 node is active, Helix must promote it to master. This behavior is achieved by providing the state priority list as MASTER,SLAVE.

STATE TRANSITION PRIORITY
Helix tries to fire as many transitions as possible in parallel to reach the stable state without violating constraints. By default Helix simply sorts the transitions alphabetically and fires as many as it can without violating the constraints. 
One can control this by overriding the priority order.
 
### Config management

Helix allows applications to store application specific properties. The configuration can have different scopes.

* Cluster
* Node specific
* Resource specific
* Partition specific

Helix also provides notifications when any configs are changed. This allows applications to support dynamic configuration changes.

See HelixManager.getConfigAccessor for more info

### Intra cluster messaging api

This is an interesting feature which is quite useful in practice. Often times, nodes in DDS requires a mechanism to interact with each other. One such requirement is a process of bootstrapping a replica.

Consider a search system use case where the index replica starts up and it does not have an index. One of the commonly used solutions is to get the index from a common location or to copy the index from another replica.
Helix provides a messaging api, that can be used to talk to other nodes in the system. The value added that Helix provides here is, message recipient can be specified in terms of resource, 
partition, state and Helix ensures that the message is delivered to all of the required recipients. In this particular use case, the instance can specify the recipient criteria as all replicas of P1. 
Since Helix is aware of the global state of the system, it can send the message to appropriate nodes. Once the nodes respond Helix provides the bootstrapping replica with all the responses.

This is a very generic api and can also be used to schedule various periodic tasks in the cluster like data backups etc. 
System Admins can also perform adhoc tasks like on demand backup or execute a system command(like rm -rf ;-)) across all nodes.

```
      ClusterMessagingService messagingService = manager.getMessagingService();
      //CONSTRUCT THE MESSAGE
      Message requestBackupUriRequest = new Message(
          MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
      requestBackupUriRequest
          .setMsgSubType(BootstrapProcess.REQUEST_BOOTSTRAP_URL);
      requestBackupUriRequest.setMsgState(MessageState.NEW);
      //SET THE RECIPIENT CRITERIA, All nodes that satisfy the criteria will receive the message
      Criteria recipientCriteria = new Criteria();
      recipientCriteria.setInstanceName("%");
      recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
      recipientCriteria.setResource("MyDB");
      recipientCriteria.setPartition("");
      //Should be processed only the process that is active at the time of sending the message. 
      //This means if the recipient is restarted after message is sent, it will not be processed.
      recipientCriteria.setSessionSpecific(true);
      // wait for 30 seconds
      int timeout = 30000;
      //The handler that will be invoked when any recipient responds to the message.
      BootstrapReplyHandler responseHandler = new BootstrapReplyHandler();
      //This will return only after all recipients respond or after timeout.
      int sentMessageCount = messagingService.sendAndWait(recipientCriteria,
          requestBackupUriRequest, responseHandler, timeout);
```

See HelixManager.getMessagingService for more info.


### Application specific property storage

There are several usecases where applications needs support for distributed data structures. Helix uses Zookeeper to store the application data and hence provides notifications when the data changes. 
One value add Helix provides is the ability to specify cache the data and also write through cache. This is more efficient than reading from ZK every time.

See HelixManager.getHelixPropertyStore

### Throttling

Since all state changes in the system are triggered through transitions, Helix can control the number of transitions that can happen in parallel. Some of the transitions may be light weight but some might involve moving data around which is quite expensive.
Helix allows applications to set threshold on transitions. The threshold can be set at the multiple scopes.

* MessageType e.g STATE_TRANSITION
* TransitionType e.g SLAVE-MASTER
* Resource e.g database
* Node i.e per node max transitions in parallel.

See HelixManager.getHelixAdmin.addMessageConstraint() 

### Health monitoring and alerting

This in currently in development mode, not yet productionized.

Helix provides ability for each node in the system to report health metrics on a periodic basis. 
Helix supports multiple ways to aggregate these metrics like simple SUM, AVG, EXPONENTIAL DECAY, WINDOW. Helix will only persist the aggregated value.
Applications can define threshold on the aggregate values according to the SLA's and when the SLA is violated Helix will fire an alert. 
Currently Helix only fires an alert but eventually we plan to use this metrics to either mark the node dead or load balance the partitions. 
This feature will be valuable in for distributed systems that support multi-tenancy and have huge variation in work load patterns. Another place this can be used is to detect skewed partitions and rebalance the cluster.

This feature is not yet stable and do not recommend to be used in production.


### Controller deployment modes

Read Architecture wiki for more details on the Role of a controller. In simple words, it basically controls the participants in the cluster by issuing transitions.

Helix provides multiple options to deploy the controller.

#### STANDALONE

Controller can be started as a separate process to manage a cluster. This is the recommended approach. How ever since one controller can be a single point of failure, multiple controller processes are required for reliability.
Even if multiple controllers are running only one will be actively managing the cluster at any time and is decided by a leader election process. If the leader fails, another leader will resume managing the cluster.

Even though we recommend this method of deployment, it has the drawback of having to manage an additional service for each cluster. See Controller As a Service option.

#### EMBEDDED

If setting up a separate controller process is not viable, then it is possible to embed the controller as a library in each of the participant. 

#### CONTROLLER AS A SERVICE

One of the cool feature we added in helix was use a set of controllers to manage a large number of clusters. 
For example if you have X clusters to be managed, instead of deploying X*3(3 controllers for fault tolerance) controllers for each cluster, one can deploy only 3 controllers. Each controller can manage X/3 clusters. 
If any controller fails the remaining two will manage X/2 clusters. At LinkedIn, we always deploy controllers in this mode. 







 
