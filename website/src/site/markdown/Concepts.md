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
  <title>Concepts</title>
</head>

Concepts
----------------------------

Helix is based on the idea that a given task has the following attributes associated with it:

* __Location__, e.g. it is available on Node N1
* __State__, e.g. it is running, stopped etc.

In Helix terminology, a task is referred to as a __resource__.

### Ideal State

An __IdealState__ allows one to map tasks to location and state. A standard way of expressing this in Helix is as follows:

```
"TASK_NAME" : {
  "LOCATION" : "STATE"
}
```

Consider a simple case where you want to launch a resource \"myTask\" on node \"N1\". The IdealState for this can be expressed as follows:

```
{
  "id" : "MyTask",
  "mapFields" : {
    "myTask" : {
      "N1" : "ONLINE",
    }
  }
}
```

### Partition

If this task get too big to fit on one box, you might want to divide it into subtasks. Each subtask is referred to as a __partition__ in Helix. Let\'s say you want to divide the task into 3 subtasks/partitions, the IdealState can be changed as shown below.

\"myTask_0\", \"myTask_1\", \"myTask_2\" are logical names representing the partitions of myTask. Each tasks runs on N1, N2 and N3 respectively.

```
{
  "id" : "myTask",
  "simpleFields" : {
    "NUM_PARTITIONS" : "3",
  }
 "mapFields" : {
    "myTask_0" : {
      "N1" : "ONLINE",
    },
    "myTask_1" : {
      "N2" : "ONLINE",
    },
    "myTask_2" : {
      "N3" : "ONLINE",
    }
  }
}
```

### Replica

Partitioning allows one to split the data/task into multiple subparts. But let\'s say the request rate for each partition increases. The common solution is to have multiple copies for each partition. Helix refers to the copy of a partition as a __replica__.  Adding a replica also increases the availability of the system during failures. One can see this methodology employed often in search systems. The index is divided into shards, and each shard has multiple copies.

Let\'s say you want to add one additional replica for each task. The IdealState can simply be changed as shown below.

For increasing the availability of the system, it\'s better to place the replica of a given partition on different nodes.

```
{
  "id" : "myIndex",
  "simpleFields" : {
    "NUM_PARTITIONS" : "3",
    "REPLICAS" : "2",
  },
 "mapFields" : {
    "myIndex_0" : {
      "N1" : "ONLINE",
      "N2" : "ONLINE"
    },
    "myIndex_1" : {
      "N2" : "ONLINE",
      "N3" : "ONLINE"
    },
    "myIndex_2" : {
      "N3" : "ONLINE",
      "N1" : "ONLINE"
    }
  }
}
```

### State

Now let\'s take a slightly more complicated scenario where a task represents a database.  Unlike an index which is in general read-only, a database supports both reads and writes. Keeping the data consistent among the replicas is crucial in distributed data stores. One commonly applied technique is to assign one replica as the MASTER and remaining replicas as SLAVEs. All writes go to the MASTER and are then replicated to the SLAVE replicas.

Helix allows one to assign different __states__ to each replica. Let\'s say you have two MySQL instances N1 and N2, where one will serve as MASTER and another as SLAVE. The IdealState can be changed to:

```
{
  "id" : "myDB",
  "simpleFields" : {
    "NUM_PARTITIONS" : "1",
    "REPLICAS" : "2",
  },
  "mapFields" : {
    "myDB" : {
      "N1" : "MASTER",
      "N2" : "SLAVE",
    }
  }
}
```


### State Machine and Transitions

The IdealState allows one to exactly specify the desired state of the cluster. Given an IdealState, Helix takes up the responsibility of ensuring that the cluster reaches the IdealState.  The Helix __controller__ reads the IdealState and then commands each Participant to take appropriate actions to move from one state to another until it matches the IdealState.  These actions are referred to as __transitions__ in Helix.

The next logical question is: how does the controller compute the transitions required to get to IdealState?  This is where the __finite state machine__ concept comes in. Helix allows applications to plug in a finite state machine.  A state machine consists of the following:

* __State__: Describes the role of a replica
* __Transition__: An action that allows a replica to move from one state to another, thus changing its role.

Here is an example of MasterSlave state machine:

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

Helix allows each resource to be associated with one state machine. This means you can have one resource as an index and another as a database in the same cluster. One can associate each resource with a state machine as follows:

```
{
  "id" : "myDB",
  "simpleFields" : {
    "NUM_PARTITIONS" : "1",
    "REPLICAS" : "2",
    "STATE_MODEL_DEF_REF" : "MasterSlave",
  },
  "mapFields" : {
    "myDB" : {
      "N1" : "MASTER",
      "N2" : "SLAVE",
    }
  }
}

```

### Current State

The __CurrentState__ of a resource simply represents its actual state at a participating node, a __participant__. In the below example:

* INSTANCE_NAME: Unique name representing the process
* SESSION_ID: ID that is automatically assigned every time a process joins the cluster

```
{
  "id":"MyResource"
  ,"simpleFields":{
    ,"SESSION_ID":"13d0e34675e0002"
    ,"INSTANCE_NAME":"node1"
    ,"STATE_MODEL_DEF":"MasterSlave"
  }
  ,"mapFields":{
    "MyResource_0":{
      "CURRENT_STATE":"SLAVE"
    }
    ,"MyResource_1":{
      "CURRENT_STATE":"MASTER"
    }
    ,"MyResource_2":{
      "CURRENT_STATE":"MASTER"
    }
  }
}
```

Each node in the cluster has its own CurrentState.

### External View

In order to communicate with the participants, external clients need to know the current state of each of the participants. The external clients are referred to as __spectators__. In order to make the life of spectator simple, Helix provides an ExternalView that is an aggregated view of the current state across all nodes. The ExternalView has a similar format as IdealState.

```
{
  "id":"MyResource",
  "mapFields":{
    "MyResource_0":{
      "N1":"SLAVE",
      "N2":"MASTER",
      "N3":"OFFLINE"
    },
    "MyResource_1":{
      "N1":"MASTER",
      "N2":"SLAVE",
      "N3":"ERROR"
    },
    "MyResource_2":{
      "N1":"MASTER",
      "N2":"SLAVE",
      "N3":"SLAVE"
    }
  }
}
```

### Rebalancer

The core component of Helix is the Controller which runs the Rebalancer algorithm on every cluster event. Cluster events can be one of the following:

* Nodes start and/or stop
* Nodes experience soft and/or hard failures
* New nodes are added/removed
* Ideal state changes

There are few more examples such as configuration changes, etc.  The key takeaway: there are many ways to trigger the rebalancer.

When a rebalancer is run it simply does the following:

* Compares the ideal state and current state
* Computes the transitions required to reach the ideal state
* Issues the transitions to each participant

The above steps happen for every change in the system. Once the current state matches the IdealState, the system is considered stable which implies \'IdealState = CurrentState = ExternalView\'

### Dynamic IdealState

One of the things that makes Helix powerful is that IdealState can be changed dynamically. This means one can listen to cluster events like node failures and dynamically change the ideal state. Helix will then take care of triggering the respective transitions in the system.

Helix allows various granularities of control for adjusting the ideal state. Whenever a cluster event occurs, Helix can operate in one of three modes:

* __FULL\_AUTO__: Helix will automatically determine the location and state of each replica based on constraints
* __SEMI\_AUTO__: Helix will take in a \"preference list\" representing the location each replica can live at, and automatically determine the state based on constraints
* __CUSTOMIZED__: Helix will take in a map of location to state and fire transitions to get the external view to match

Helix comes with a few algorithms to automatically compute the IdealState based on the constraints. For example, if you have a resource of 3 partitions and 2 replicas, Helix can automatically compute the IdealState based on the nodes that are currently active. See the [tutorial](./0.6.4-docs/tutorial_rebalance.html) to find out more about various execution modes of Helix like FULL_AUTO, SEMI_AUTO and CUSTOMIZED.












