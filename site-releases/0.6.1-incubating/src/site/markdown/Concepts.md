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

Helix is based on the idea that a given task has the following attributes associated with it:

* _Location of the task_. For example it runs on Node N1
* _State_. For example, it is running, stopped etc.

In Helix terminology, a task is referred to as a _resource_.

### IdealState

IdealState simply allows one to map tasks to location and state. A standard way of expressing this in Helix:

```
  "TASK_NAME" : {
    "LOCATION" : "STATE"
  }

```
Consider a simple case where you want to launch a task \'myTask\' on node \'N1\'. The IdealState for this can be expressed as follows:

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

If this task get too big to fit on one box, you might want to divide it into subTasks. Each subTask is referred to as a _partition_ in Helix. Let\'s say you want to divide the task into 3 subTasks/partitions, the IdealState can be changed as shown below. 

\'myTask_0\', \'myTask_1\', \'myTask_2\' are logical names representing the partitions of myTask. Each tasks runs on N1, N2 and N3 respectively.

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

Partitioning allows one to split the data/task into multiple subparts. But let\'s say the request rate each partition increases. The common solution is to have multiple copies for each partition. Helix refers to the copy of a partition as a _replica_.  Adding a replica also increases the availability of the system during failures. One can see this methodology employed often in Search systems. The index is divided into shards, and each shard has multiple copies.

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

Now let\'s take a slightly complicated scenario where a task represents a database.  Unlike an index which is in general read-only, a database supports both reads and writes. Keeping the data consistent among the replicas is crucial in distributed data stores. One commonly applied technique is to assign one replica as MASTER and remaining replicas as SLAVE. All writes go to the MASTER and are then replicated to the SLAVE replicas.

Helix allows one to assign different states to each replica. Let\'s say you have two MySQL instances N1 and N2, where one will serve as MASTER and another as SLAVE. The IdealState can be changed to:

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

IdealState allows one to exactly specify the desired state of the cluster. Given an IdealState, Helix takes up the responsibility of ensuring that the cluster reaches the IdealState.  The Helix _controller_ reads the IdealState and then commands the Participant to take appropriate actions to move from one state to another until it matches the IdealState.  These actions are referred to as _transitions_ in Helix.

The next logical question is:  how does the _controller_ compute the transitions required to get to IdealState?  This is where the finite state machine concept comes in. Helix allows applications to plug in a finite state machine.  A state machine consists of the following:

* State: Describes the role of a replica
* Transition: An action that allows a replica to move from one State to another, thus changing its role.

Here is an example of MASTERSLAVE state machine,

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

CurrentState of a resource simply represents its actual state at a PARTICIPANT. In the below example:

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

In order to communicate with the PARTICIPANTs, external clients need to know the current state of each of the PARTICIPANTs. The external clients are referred to as SPECTATORS. In order to make the life of SPECTATOR simple, Helix provides an EXTERNALVIEW that is an aggregated view of the current state across all nodes. The EXTERNALVIEW has a similar format as IDEALSTATE.

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

The core component of Helix is the CONTROLLER which runs the REBALANCER algorithm on every cluster event. Cluster events can be one of the following:

* Nodes start/stop and soft/hard failures
* New nodes are added/removed
* Ideal state changes

There are few more such as config changes, etc.  The key takeaway: there are many ways to trigger the rebalancer.

When a rebalancer is run it simply does the following:

* Compares the IdealState and current state
* Computes the transitions required to reach the IdealState
* Issues the transitions to each PARTICIPANT

The above steps happen for every change in the system. Once the current state matches the IdealState, the system is considered stable which implies \'IDEALSTATE = CURRENTSTATE = EXTERNALVIEW\'

### Dynamic IdealState

One of the things that makes Helix powerful is that IdealState can be changed dynamically. This means one can listen to cluster events like node failures and dynamically change the ideal state. Helix will then take care of triggering the respective transitions in the system.

Helix comes with a few algorithms to automatically compute the IdealState based on the constraints. For example, if you have a resource of 3 partitions and 2 replicas, Helix can automatically compute the IdealState based on the nodes that are currently active. See the [tutorial](./tutorial_rebalance.html) to find out more about various execution modes of Helix like AUTO_REBALANCE, AUTO and CUSTOM. 












