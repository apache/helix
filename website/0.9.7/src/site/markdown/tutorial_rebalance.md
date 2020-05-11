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
  <title>Tutorial - Rebalancing Algorithms</title>
</head>

## [Helix Tutorial](./Tutorial.html): Rebalancing Algorithms

The placement of partitions in a distributed system is essential for the reliability and scalability of the system.  For example, when a node fails, it is important that the partitions hosted on that node are reallocated evenly among the remaining nodes. Consistent hashing is one such algorithm that can satisfy this guarantee.  Helix provides a variant of consistent hashing based on the RUSH algorithm, among others.

This means given a number of partitions, replicas and number of nodes, Helix does the automatic assignment of partition to nodes such that:

* Each node has the same number of partitions
* Replicas of the same partition do not stay on the same node
* When a node fails, the partitions will be equally distributed among the remaining nodes
* When new nodes are added, the number of partitions moved will be minimized along with satisfying the above criteria

Helix employs a rebalancing algorithm to compute the _ideal state_ of the system.  When the _current state_ differs from the _ideal state_, Helix uses it as the target state of the system and computes the appropriate transitions needed to bring it to the _ideal state_.

Helix makes it easy to perform this operation, while giving you control over the algorithm.  In this section, we\'ll see how to implement the desired behavior.

Helix has four options for rebalancing, in increasing order of customization by the system builder:

* FULL_AUTO
* SEMI_AUTO
* CUSTOMIZED
* USER_DEFINED

```
            |FULL_AUTO     |  SEMI_AUTO | CUSTOMIZED|  USER_DEFINED  |
            ---------------------------------------------------------|
   LOCATION | HELIX        |  APP       |  APP      |      APP       |
            ---------------------------------------------------------|
      STATE | HELIX        |  HELIX     |  APP      |      APP       |
            ----------------------------------------------------------
```


### FULL_AUTO

When the rebalance mode is set to FULL_AUTO, Helix controls both the location of the replica along with the state. This option is useful for applications where creation of a replica is not expensive.

For example, consider this system that uses a MasterSlave state model, with 3 partitions and 2 replicas in the ideal state.

```
{
  "id" : "MyResource",
  "simpleFields" : {
    "REBALANCE_MODE" : "FULL_AUTO",
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

If there are 3 nodes in the cluster, then Helix will balance the masters and slaves equally.  The ideal state is therefore:

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

Another typical example is evenly distributing a group of tasks among the currently healthy processes. For example, if there are 60 tasks and 4 nodes, Helix assigns 15 tasks to each node.
When one node fails, Helix redistributes its 15 tasks to the remaining 3 nodes, resulting in a balanced 20 tasks per node. Similarly, if a node is added, Helix re-allocates 3 tasks from each of the 4 nodes to the 5th node, resulting in a balanced distribution of 12 tasks per node..

### SEMI_AUTO

When the application needs to control the placement of the replicas, use the SEMI_AUTO rebalance mode.

Example: In the ideal state below, the partition \'MyResource_0\' is constrained to be placed only on node1 or node2.  The choice of _state_ is still controlled by Helix.  That means MyResource_0.MASTER could be on node1 and MyResource_0.SLAVE on node2, or vice-versa but neither would be placed on node3.

```
{
  "id" : "MyResource",
  "simpleFields" : {
    "REBALANCE_MODE" : "SEMI_AUTO",
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

The MasterSlave state model requires that a partition has exactly one MASTER at all times, and the other replicas should be SLAVEs.  In this simple example with 2 replicas per partition, there would be one MASTER and one SLAVE.  Upon failover, a SLAVE has to assume mastership, and a new SLAVE will be generated.

In this mode when node1 fails, unlike in FULL_AUTO mode the partition is _not_ moved from node1 to node3. Instead, Helix will decide to change the state of MyResource_0 on node2 from SLAVE to MASTER, based on the system constraints.

### CUSTOMIZED

Helix offers a third mode called CUSTOMIZED, in which the application controls the placement _and_ state of each replica. The application needs to implement a callback interface that Helix invokes when the cluster state changes.
Within this callback, the application can recompute the idealstate. Helix will then issue appropriate transitions such that _Idealstate_ and _Currentstate_ converges.

Here\'s an example, again with 3 partitions, 2 replicas per partition, and the MasterSlave state model:

```
{
  "id" : "MyResource",
  "simpleFields" : {
    "REBALANCE_MODE" : "CUSTOMIZED",
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

Suppose the current state of the system is 'MyResource_0' \-\> {N1:MASTER, N2:SLAVE} and the application changes the ideal state to 'MyResource_0' \-\> {N1:SLAVE,N2:MASTER}. While the application decides which node is MASTER and which is SLAVE, Helix will not blindly issue MASTER\-\-\>SLAVE to N1 and SLAVE\-\-\>MASTER to N2 in parallel, since that might result in a transient state where both N1 and N2 are masters, which violates the MasterSlave constraint that there is exactly one MASTER at a time.  Helix will first issue MASTER\-\-\>SLAVE to N1 and after it is completed, it will issue SLAVE\-\-\>MASTER to N2.

### USER_DEFINED

For maximum flexibility, Helix exposes an interface that can allow applications to plug in custom rebalancing logic. By providing the name of a class that implements the Rebalancer interface, Helix will automatically call the contained method whenever there is a change to the live participants in the cluster. For more, see [User-Defined Rebalancer](./tutorial_user_def_rebalancer.html).

### Backwards Compatibility

In previous versions, FULL_AUTO was called AUTO_REBALANCE and SEMI_AUTO was called AUTO. Furthermore, they were presented as the IDEAL_STATE_MODE. Helix supports both IDEAL_STATE_MODE and REBALANCE_MODE, but IDEAL_STATE_MODE is now deprecated and may be phased out in future versions.
