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
  <title>Quickstart</title>
</head>

Quickstart
---------

Get Helix
---------

First, let\'s get Helix. Either build it, or download it.

### Build

```
git clone https://git-wip-us.apache.org/repos/asf/helix.git
cd helix
git checkout tags/helix-0.7.1
mvn install package -DskipTests
# This folder contains quickstart.sh and start-helix-participant.sh
cd helix-examples/target/helix-examples-pkg/bin
chmod +x *
# This folder contains helix-admin.sh, start-standalone-zookeeper.sh, and run-helix-controller.sh
cd ../../../../helix-core/target/helix-core-pkg/bin
```

### Download

Download the 0.7.1 release package [here](./download.html)

Overview
--------

In this Quickstart, we\'ll set up a master-slave replicated, partitioned system.  Then we\'ll demonstrate how to add a node, rebalance the partitions, and show how Helix manages failover.


Let\'s Do It
------------

Helix provides command line interfaces to set up the cluster and view the cluster state. The best way to understand how Helix views a cluster is to build a cluster.

### Get to the Tools Directory

If you built the code:

```
cd helix/helix/helix-examples/target/helix-examples-pkg/bin
```

If you downloaded the release package, extract it.


Short Version
-------------
You can observe the components working together in this demo, which does the following:

* Create a cluster
* Add 2 nodes (participants) to the cluster
* Set up a resource with 6 partitions and 2 replicas: 1 Master, and 1 Slave per partition
* Show the cluster state after Helix balances the partitions
* Add a third node
* Show the cluster state.  Note that the third node has taken mastership of 2 partitions.
* Kill the third node (Helix takes care of failover)
* Show the cluster state.  Note that the two surviving nodes take over mastership of the partitions from the failed node

### Run the Demo

```
cd helix/helix/helix-examples/target/helix-examples-pkg/bin
./quickstart.sh
```

#### The Initial Setup

2 nodes are set up and the partitions are rebalanced.

The cluster state is as follows:

```
CLUSTER STATE: After starting 2 nodes
                localhost_12000    localhost_12001
MyResource_0           M                  S
MyResource_1           S                  M
MyResource_2           M                  S
MyResource_3           M                  S
MyResource_4           S                  M
MyResource_5           S                  M
```

Note there is one master and one slave per partition.

#### Add a Node

A third node is added and the cluster is rebalanced.

The cluster state changes to:

```
CLUSTER STATE: After adding a third node
               localhost_12000    localhost_12001    localhost_12002
MyResource_0          S                  M                  S
MyResource_1          S                  S                  M
MyResource_2          M                  S                  S
MyResource_3          S                  S                  M
MyResource_4          M                  S                  S
MyResource_5          S                  M                  S
```

Note there is one master and _two_ slaves per partition.  This is expected because there are three nodes.

#### Kill a Node

Finally, a node is killed to simulate a failure

Helix makes sure each partition has a master.  The cluster state changes to:

```
CLUSTER STATE: After the 3rd node stops/crashes
               localhost_12000    localhost_12001    localhost_12002
MyResource_0          S                  M                  -
MyResource_1          S                  M                  -
MyResource_2          M                  S                  -
MyResource_3          M                  S                  -
MyResource_4          M                  S                  -
MyResource_5          S                  M                  -
```


Long Version
------------
Now you can run the same steps by hand.  In this detailed version, we\'ll do the following:

* Define a cluster
* Add two nodes to the cluster
* Add a 6-partition resource with 1 master and 2 slave replicas per partition
* Verify that the cluster is healthy and inspect the Helix view
* Expand the cluster: add a few nodes and rebalance the partitions
* Failover: stop a node and verify the mastership transfer

### Install and Start ZooKeeper

Zookeeper can be started in standalone mode or replicated mode.

More information is available at

* http://zookeeper.apache.org/doc/r3.3.3/zookeeperStarted.html
* http://zookeeper.apache.org/doc/trunk/zookeeperAdmin.html#sc_zkMulitServerSetup

In this example, let\'s start zookeeper in local mode.

#### Start ZooKeeper Locally on Port 2199

```
./start-standalone-zookeeper.sh 2199 &
```

### Define the Cluster

The helix-admin tool is used for cluster administration tasks. In the Quickstart, we\'ll use the command line interface. Helix supports a REST interface as well.

zookeeper_address is of the format host:port e.g localhost:2199 for standalone or host1:port,host2:port for multi-node.

Next, we\'ll set up a cluster MYCLUSTER cluster with these attributes:

* 3 instances running on localhost at ports 12913,12914,12915
* One database named myDB with 6 partitions
* Each partition will have 3 replicas with 1 master, 2 slaves
* ZooKeeper running locally at localhost:2199

#### Create the Cluster MYCLUSTER

```
# ./helix-admin.sh --zkSvr <zk_address> --addCluster <clustername>
./helix-admin.sh --zkSvr localhost:2199 --addCluster MYCLUSTER
```

### Add Nodes to the Cluster

In this case we\'ll add three nodes: localhost:12913, localhost:12914, localhost:12915

```
# helix-admin.sh --zkSvr <zk_address>  --addNode <clustername> <host:port>
./helix-admin.sh --zkSvr localhost:2199  --addNode MYCLUSTER localhost:12913
./helix-admin.sh --zkSvr localhost:2199  --addNode MYCLUSTER localhost:12914
./helix-admin.sh --zkSvr localhost:2199  --addNode MYCLUSTER localhost:12915
```

### Define the Resource and Partitioning

In this example, the resource is a database, partitioned 6 ways. Note that in a production system, it\'s common to over-partition for better load balancing.  Helix has been used in production to manage hundreds of databases each with 10s or 100s of partitions running on 10s of physical nodes.

#### Create a Database with 6 Partitions using the MasterSlave State Model

Helix ensures there will be exactly one master for each partition.

```
# helix-admin.sh --zkSvr <zk_address> --addResource <clustername> <resourceName> <numPartitions> <StateModelName>
./helix-admin.sh --zkSvr localhost:2199 --addResource MYCLUSTER myDB 6 MasterSlave
```

#### Let Helix Assign Partitions to Nodes

This command will distribute the partitions amongst all the nodes in the cluster. In this example, each partition has 3 replicas.

```
# helix-admin.sh --zkSvr <zk_address> --rebalance <clustername> <resourceName> <replication factor>
./helix-admin.sh --zkSvr localhost:2199 --rebalance MYCLUSTER myDB 3
```

Now the cluster is defined in ZooKeeper.  The nodes (localhost:12913, localhost:12914, localhost:12915) and resource (myDB, with 6 partitions using the MasterSlave model) are all properly configured.  And the _IdealState_ has been calculated, assuming a replication factor of 3.

### Start the Helix Controller

Now that the cluster is defined in ZooKeeper, the Helix controller can manage the cluster.

```
# Start the cluster manager, which will manage MYCLUSTER
./run-helix-controller.sh --zkSvr localhost:2199 --cluster MYCLUSTER 2>&1 > /tmp/controller.log &
```

### Start up the Cluster to be Managed

We\'ve started up ZooKeeper, defined the cluster, the resources, the partitioning, and started up the Helix controller.  Next, we\'ll start up the nodes of the system to be managed.  Each node is a Participant, which is an instance of the system component to be managed.  Helix assigns work to Participants, keeps track of their roles and health, and takes action when a node fails.

```
# start up each instance.  These are mock implementations that are actively managed by Helix
./start-helix-participant.sh --zkSvr localhost:2199 --cluster MYCLUSTER --host localhost --port 12913 --stateModelType MasterSlave 2>&1 > /tmp/participant_12913.log
./start-helix-participant.sh --zkSvr localhost:2199 --cluster MYCLUSTER --host localhost --port 12914 --stateModelType MasterSlave 2>&1 > /tmp/participant_12914.log
./start-helix-participant.sh --zkSvr localhost:2199 --cluster MYCLUSTER --host localhost --port 12915 --stateModelType MasterSlave 2>&1 > /tmp/participant_12915.log
```

### Inspect the Cluster

Now, let\'s see the Helix view of our cluster.  We\'ll work our way down as follows:

```
Clusters -> MYCLUSTER -> instances -> instance detail
                      -> resources -> resource detail
                      -> partitions
```

A single Helix controller can manage multiple clusters, though so far, we\'ve only defined one cluster.  Let\'s see:

```
# List existing clusters
./helix-admin.sh --zkSvr localhost:2199 --listClusters

Existing clusters:
MYCLUSTER
```

Now, let\'s see the Helix view of MYCLUSTER:

```
# helix-admin.sh --zkSvr <zk_address> --listClusterInfo <clusterName>
./helix-admin.sh --zkSvr localhost:2199 --listClusterInfo MYCLUSTER

Existing resources in cluster MYCLUSTER:
myDB
Instances in cluster MYCLUSTER:
localhost_12915
localhost_12914
localhost_12913
```

Let\'s look at the details of an instance:

```
# ./helix-admin.sh --zkSvr <zk_address> --listInstanceInfo <clusterName> <InstanceName>
./helix-admin.sh --zkSvr localhost:2199 --listInstanceInfo MYCLUSTER localhost_12913

InstanceConfig: {
  "id" : "localhost_12913",
  "mapFields" : {
  },
  "listFields" : {
  },
  "simpleFields" : {
    "HELIX_ENABLED" : "true",
    "HELIX_HOST" : "localhost",
    "HELIX_PORT" : "12913"
  }
}
```


#### Query Information about a Resource

```
# helix-admin.sh --zkSvr <zk_address> --listResourceInfo <clusterName> <resourceName>
./helix-admin.sh --zkSvr localhost:2199 --listResourceInfo MYCLUSTER myDB

IdealState for myDB:
{
  "id" : "myDB",
  "mapFields" : {
    "myDB_0" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "MASTER",
      "localhost_12915" : "SLAVE"
    },
    "myDB_1" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "SLAVE",
      "localhost_12915" : "MASTER"
    },
    "myDB_2" : {
      "localhost_12913" : "MASTER",
      "localhost_12914" : "SLAVE",
      "localhost_12915" : "SLAVE"
    },
    "myDB_3" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "SLAVE",
      "localhost_12915" : "MASTER"
    },
    "myDB_4" : {
      "localhost_12913" : "MASTER",
      "localhost_12914" : "SLAVE",
      "localhost_12915" : "SLAVE"
    },
    "myDB_5" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "MASTER",
      "localhost_12915" : "SLAVE"
    }
  },
  "listFields" : {
    "myDB_0" : [ "localhost_12914", "localhost_12913", "localhost_12915" ],
    "myDB_1" : [ "localhost_12915", "localhost_12913", "localhost_12914" ],
    "myDB_2" : [ "localhost_12913", "localhost_12915", "localhost_12914" ],
    "myDB_3" : [ "localhost_12915", "localhost_12913", "localhost_12914" ],
    "myDB_4" : [ "localhost_12913", "localhost_12914", "localhost_12915" ],
    "myDB_5" : [ "localhost_12914", "localhost_12915", "localhost_12913" ]
  },
  "simpleFields" : {
    "IDEAL_STATE_MODE" : "AUTO",
    "REBALANCE_MODE" : "SEMI_AUTO",
    "NUM_PARTITIONS" : "6",
    "REPLICAS" : "3",
    "STATE_MODEL_DEF_REF" : "MasterSlave",
    "STATE_MODEL_FACTORY_NAME" : "DEFAULT"
  }
}

ExternalView for myDB:
{
  "id" : "myDB",
  "mapFields" : {
    "myDB_0" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "MASTER",
      "localhost_12915" : "SLAVE"
    },
    "myDB_1" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "SLAVE",
      "localhost_12915" : "MASTER"
    },
    "myDB_2" : {
      "localhost_12913" : "MASTER",
      "localhost_12914" : "SLAVE",
      "localhost_12915" : "SLAVE"
    },
    "myDB_3" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "SLAVE",
      "localhost_12915" : "MASTER"
    },
    "myDB_4" : {
      "localhost_12913" : "MASTER",
      "localhost_12914" : "SLAVE",
      "localhost_12915" : "SLAVE"
    },
    "myDB_5" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "MASTER",
      "localhost_12915" : "SLAVE"
    }
  },
  "listFields" : {
  },
  "simpleFields" : {
    "BUCKET_SIZE" : "0"
  }
}
```

Now, let\'s look at one of the partitions:

```
# helix-admin.sh --zkSvr <zk_address> --listResourceInfo <clusterName> <partition>
./helix-admin.sh --zkSvr localhost:2199 --listResourceInfo mycluster myDB_0
```

### Expand the Cluster

Next, we\'ll show how Helix does the work that you\'d otherwise have to build into your system.  When you add capacity to your cluster, you want the work to be evenly distributed.  In this example, we started with 3 nodes, with 6 partitions.  The partitions were evenly balanced, 2 masters and 4 slaves per node. Let\'s add 3 more nodes: localhost:12916, localhost:12917, localhost:12918

```
./helix-admin.sh --zkSvr localhost:2199  --addNode MYCLUSTER localhost:12916
./helix-admin.sh --zkSvr localhost:2199  --addNode MYCLUSTER localhost:12917
./helix-admin.sh --zkSvr localhost:2199  --addNode MYCLUSTER localhost:12918
```

And start up these instances:

```
# start up each instance.  These are mock implementations that are actively managed by Helix
./start-helix-participant.sh --zkSvr localhost:2199 --cluster MYCLUSTER --host localhost --port 12916 --stateModelType MasterSlave 2>&1 > /tmp/participant_12916.log
./start-helix-participant.sh --zkSvr localhost:2199 --cluster MYCLUSTER --host localhost --port 12917 --stateModelType MasterSlave 2>&1 > /tmp/participant_12917.log
./start-helix-participant.sh --zkSvr localhost:2199 --cluster MYCLUSTER --host localhost --port 12918 --stateModelType MasterSlave 2>&1 > /tmp/participant_12918.log
```


And now, let Helix do the work for you.  To shift the work, simply rebalance.  After the rebalance, each node will have one master and two slaves.

```
./helix-admin.sh --zkSvr localhost:2199 --rebalance MYCLUSTER myDB 3
```

### View the Cluster

OK, let\'s see how it looks:


```
./helix-admin.sh --zkSvr localhost:2199 --listResourceInfo MYCLUSTER myDB

IdealState for myDB:
{
  "id" : "myDB",
  "mapFields" : {
    "myDB_0" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "SLAVE",
      "localhost_12917" : "MASTER"
    },
    "myDB_1" : {
      "localhost_12916" : "SLAVE",
      "localhost_12917" : "SLAVE",
      "localhost_12918" : "MASTER"
    },
    "myDB_2" : {
      "localhost_12913" : "MASTER",
      "localhost_12917" : "SLAVE",
      "localhost_12918" : "SLAVE"
    },
    "myDB_3" : {
      "localhost_12915" : "MASTER",
      "localhost_12917" : "SLAVE",
      "localhost_12918" : "SLAVE"
    },
    "myDB_4" : {
      "localhost_12916" : "MASTER",
      "localhost_12917" : "SLAVE",
      "localhost_12918" : "SLAVE"
    },
    "myDB_5" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "MASTER",
      "localhost_12915" : "SLAVE"
    }
  },
  "listFields" : {
    "myDB_0" : [ "localhost_12917", "localhost_12913", "localhost_12914" ],
    "myDB_1" : [ "localhost_12918", "localhost_12917", "localhost_12916" ],
    "myDB_2" : [ "localhost_12913", "localhost_12917", "localhost_12918" ],
    "myDB_3" : [ "localhost_12915", "localhost_12917", "localhost_12918" ],
    "myDB_4" : [ "localhost_12916", "localhost_12917", "localhost_12918" ],
    "myDB_5" : [ "localhost_12914", "localhost_12915", "localhost_12913" ]
  },
  "simpleFields" : {
    "IDEAL_STATE_MODE" : "AUTO",
    "REBALANCE_MODE" : "SEMI_AUTO",
    "NUM_PARTITIONS" : "6",
    "REPLICAS" : "3",
    "STATE_MODEL_DEF_REF" : "MasterSlave",
    "STATE_MODEL_FACTORY_NAME" : "DEFAULT"
  }
}

ExternalView for myDB:
{
  "id" : "myDB",
  "mapFields" : {
    "myDB_0" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "SLAVE",
      "localhost_12917" : "MASTER"
    },
    "myDB_1" : {
      "localhost_12916" : "SLAVE",
      "localhost_12917" : "SLAVE",
      "localhost_12918" : "MASTER"
    },
    "myDB_2" : {
      "localhost_12913" : "MASTER",
      "localhost_12917" : "SLAVE",
      "localhost_12918" : "SLAVE"
    },
    "myDB_3" : {
      "localhost_12915" : "MASTER",
      "localhost_12917" : "SLAVE",
      "localhost_12918" : "SLAVE"
    },
    "myDB_4" : {
      "localhost_12916" : "MASTER",
      "localhost_12917" : "SLAVE",
      "localhost_12918" : "SLAVE"
    },
    "myDB_5" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "MASTER",
      "localhost_12915" : "SLAVE"
    }
  },
  "listFields" : {
  },
  "simpleFields" : {
    "BUCKET_SIZE" : "0"
  }
}
```

Mission accomplished.  The partitions are nicely balanced.

### How about Failover?

Building a fault tolerant system isn\'t trivial, but with Helix, it\'s easy.  Helix detects a failed instance, and triggers mastership transfer automatically.

First, let's fail an instance.  In this example, we\'ll kill localhost:12918 to simulate a failure.

We lost localhost:12918, so myDB_1 lost its MASTER.  Helix can fix that, it will transfer mastership to a healthy node that is currently a SLAVE, say localhost:12197.  Helix balances the load as best as it can, given there are 6 partitions on 5 nodes.  Let\'s see:


```
./helix-admin.sh --zkSvr localhost:2199 --listResourceInfo MYCLUSTER myDB

IdealState for myDB:
{
  "id" : "myDB",
  "mapFields" : {
    "myDB_0" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "SLAVE",
      "localhost_12917" : "MASTER"
    },
    "myDB_1" : {
      "localhost_12916" : "SLAVE",
      "localhost_12917" : "SLAVE",
      "localhost_12918" : "MASTER"
    },
    "myDB_2" : {
      "localhost_12913" : "MASTER",
      "localhost_12917" : "SLAVE",
      "localhost_12918" : "SLAVE"
    },
    "myDB_3" : {
      "localhost_12915" : "MASTER",
      "localhost_12917" : "SLAVE",
      "localhost_12918" : "SLAVE"
    },
    "myDB_4" : {
      "localhost_12916" : "MASTER",
      "localhost_12917" : "SLAVE",
      "localhost_12918" : "SLAVE"
    },
    "myDB_5" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "MASTER",
      "localhost_12915" : "SLAVE"
    }
  },
  "listFields" : {
    "myDB_0" : [ "localhost_12917", "localhost_12913", "localhost_12914" ],
    "myDB_1" : [ "localhost_12918", "localhost_12917", "localhost_12916" ],
    "myDB_2" : [ "localhost_12913", "localhost_12918", "localhost_12917" ],
    "myDB_3" : [ "localhost_12915", "localhost_12918", "localhost_12917" ],
    "myDB_4" : [ "localhost_12916", "localhost_12917", "localhost_12918" ],
    "myDB_5" : [ "localhost_12914", "localhost_12915", "localhost_12913" ]
  },
  "simpleFields" : {
    "IDEAL_STATE_MODE" : "AUTO",
    "REBALANCE_MODE" : "SEMI_AUTO",
    "NUM_PARTITIONS" : "6",
    "REPLICAS" : "3",
    "STATE_MODEL_DEF_REF" : "MasterSlave",
    "STATE_MODEL_FACTORY_NAME" : "DEFAULT"
  }
}

ExternalView for myDB:
{
  "id" : "myDB",
  "mapFields" : {
    "myDB_0" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "SLAVE",
      "localhost_12917" : "MASTER"
    },
    "myDB_1" : {
      "localhost_12916" : "SLAVE",
      "localhost_12917" : "MASTER"
    },
    "myDB_2" : {
      "localhost_12913" : "MASTER",
      "localhost_12917" : "SLAVE"
    },
    "myDB_3" : {
      "localhost_12915" : "MASTER",
      "localhost_12917" : "SLAVE"
    },
    "myDB_4" : {
      "localhost_12916" : "MASTER",
      "localhost_12917" : "SLAVE"
    },
    "myDB_5" : {
      "localhost_12913" : "SLAVE",
      "localhost_12914" : "MASTER",
      "localhost_12915" : "SLAVE"
    }
  },
  "listFields" : {
  },
  "simpleFields" : {
    "BUCKET_SIZE" : "0"
  }
}
```

As we\'ve seen in this Quickstart, Helix takes care of partitioning, load balancing, elasticity, failure detection and recovery.

### ZooInspector

You can view all of the underlying data by going direct to zookeeper.  Use ZooInspector that comes with zookeeper to browse the data. This is a java applet (make sure you have X windows)

To start zooinspector run the following command from <zk_install_directory>/contrib/ZooInspector

```
java -cp zookeeper-3.3.3-ZooInspector.jar:lib/jtoaster-1.0.4.jar:../../lib/log4j-1.2.15.jar:../../zookeeper-3.3.3.jar org.apache.zookeeper.inspector.ZooInspector
```

### Next

Now that you understand the idea of Helix, read the [tutorial](./Tutorial.html) to learn how to choose the right state model and constraints for your system, and how to implement it.  In many cases, the built-in features meet your requirements.  And best of all, Helix is a customizable framework, so you can plug in your own behavior, while retaining the automation provided by Helix.

