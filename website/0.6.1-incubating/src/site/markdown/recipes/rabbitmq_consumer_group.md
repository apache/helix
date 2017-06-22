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


RabbitMQ Consumer Group
-----------------------

[RabbitMQ](http://www.rabbitmq.com/) is well-known open source software the provides robust messaging for applications.

One of the commonly implemented recipes using this software is a work queue.  [http://www.rabbitmq.com/tutorials/tutorial-four-java.html](http://www.rabbitmq.com/tutorials/tutorial-four-java.html) describes the use case where

* A producer sends a message with a routing key
* The message is routed to the queue whose binding key exactly matches the routing key of the message
* There are multiple consumers and each consumer is interested in processing only a subset of the messages by binding to the interested keys

The example provided [here](http://www.rabbitmq.com/tutorials/tutorial-four-java.html) describes how multiple consumers can be started to process all the messages.

While this works, in production systems one needs the following:

* Ability to handle failures: when a consumer fails, another consumer must be started or the other consumers must start processing these messages that should have been processed by the failed consumer
* When the existing consumers cannot keep up with the task generation rate, new consumers will be added. The tasks must be redistributed among all the consumers

In this recipe, we demonstrate handling of consumer failures and new consumer additions using Helix.

Mapping this usecase to Helix is pretty easy as the binding key/routing key is equivalent to a partition.

Let's take an example. Lets say the queue has 6 partitions, and we have 2 consumers to process all the queues.
What we want is all 6 queues to be evenly divided among 2 consumers.
Eventually when the system scales, we add more consumers to keep up. This will make each consumer process tasks from 2 queues.
Now let's say that a consumer failed, reducing the number of active consumers to 2. This means each consumer must process 3 queues.

We showcase how such a dynamic application can be developed using Helix. Even though we use RabbitMQ as the pub/sub system one can extend this solution to other pub/sub systems.

### Try It

```
git clone https://git-wip-us.apache.org/repos/asf/helix.git
cd helix
git checkout tags/helix-0.6.1-incubating
mvn clean install package -DskipTests
cd recipes/rabbitmq-consumer-group/bin
chmod +x *
export HELIX_PKG_ROOT=`pwd`/helix-core/target/helix-core-pkg
export HELIX_RABBITMQ_ROOT=`pwd`/recipes/rabbitmq-consumer-group/
chmod +x $HELIX_PKG_ROOT/bin/*
chmod +x $HELIX_RABBITMQ_ROOT/bin/*
```

#### Install RabbitMQ

Setting up RabbitMQ on a local box is straightforward. You can find the instructions here
http://www.rabbitmq.com/download.html

#### Start ZK

Start ZooKeeper at port 2199

```
$HELIX_PKG_ROOT/bin/start-standalone-zookeeper 2199
```

#### Setup the Consumer Group Cluster

This will setup the cluster by creating a "rabbitmq-consumer-group" cluster and adds a "topic" with "6" queues.

```
$HELIX_RABBITMQ_ROOT/bin/setup-cluster.sh localhost:2199
```

#### Add Consumers

Start 2 consumers in 2 different terminals. Each consumer is given a unique ID.

```
//start-consumer.sh zookeeperAddress (e.g. localhost:2181) consumerId , rabbitmqServer (e.g. localhost)
$HELIX_RABBITMQ_ROOT/bin/start-consumer.sh localhost:2199 0 localhost
$HELIX_RABBITMQ_ROOT/bin/start-consumer.sh localhost:2199 1 localhost

```

#### Start the Helix Controller

Now start a Helix controller that starts managing the "rabbitmq-consumer-group" cluster.

```
$HELIX_RABBITMQ_ROOT/bin/start-cluster-manager.sh localhost:2199
```

#### Send Messages to the Topic

Start sending messages to the topic. This script randomly selects a routing key (1-6) and sends the message to topic.
Based on the key, messages gets routed to the appropriate queue.

```
$HELIX_RABBITMQ_ROOT/bin/send-message.sh localhost 20
```

After running this, you should see all 20 messages being processed by 2 consumers.

#### Add Another Consumer

Once a new consumer is started, Helix detects it. In order to balance the load between 3 consumers, it deallocates 1 partition from the existing consumers and allocates it to the new consumer. We see that
each consumer is now processing only 2 queues.
Helix makes sure that old nodes are asked to stop consuming before the new consumer is asked to start consuming for a given partition. But the transitions for each partition can happen in parallel.

```
$HELIX_RABBITMQ_ROOT/bin/start-consumer.sh localhost:2199 2 localhost
```

Send messages again to the topic

```
$HELIX_RABBITMQ_ROOT/bin/send-message.sh localhost 100
```

You should see that messages are now received by all 3 consumers.

#### Stop a Consumer

In any terminal press CTRL^C and notice that Helix detects the consumer failure and distributes the 2 partitions that were processed by failed consumer to the remaining 2 active consumers.


### How does this work?

Find the entire code [here](https://git-wip-us.apache.org/repos/asf?p=helix.git;a=tree;f=recipes/rabbitmq-consumer-group/src/main/java/org/apache/helix/recipes/rabbitmq).

#### Cluster Setup

This step creates ZNode on ZooKeeper for the cluster and adds the state model. We use online offline state model since there is no need for other states. The consumer is either processing a queue or it is not.

It creates a resource called "rabbitmq-consumer-group" with 6 partitions. The execution mode is set to AUTO_REBALANCE. This means that the Helix controls the assignment of partition to consumers and automatically distributes the partitions evenly among the active consumers. When a consumer is added or removed, it ensures that a minimum number of partitions are shuffled.

```
zkclient = new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
    ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);

// add cluster
admin.addCluster(clusterName, true);

// add state model definition
StateModelConfigGenerator generator = new StateModelConfigGenerator();
admin.addStateModelDef(clusterName, "OnlineOffline",
    new StateModelDefinition(generator.generateConfigForOnlineOffline()));

// add resource "topic" which has 6 partitions
String resourceName = "rabbitmq-consumer-group";
admin.addResource(clusterName, resourceName, 6, "OnlineOffline", "AUTO_REBALANCE");
```

### Starting the Consumers

The only thing consumers need to know is the ZooKeeper address, cluster name and consumer ID. It does not need to know anything else.

```
_manager = HelixManagerFactory.getZKHelixManager(_clusterName,
                                                 _consumerId,
                                                 InstanceType.PARTICIPANT,
                                                 _zkAddr);

StateMachineEngine stateMach = _manager.getStateMachineEngine();
ConsumerStateModelFactory modelFactory =
    new ConsumerStateModelFactory(_consumerId, _mqServer);
stateMach.registerStateModelFactory("OnlineOffline", modelFactory);

_manager.connect();
```

Once the consumer has registered the state model and the controller is started, the consumer starts getting callbacks (onBecomeOnlineFromOffline) for the partition it needs to host. All it needs to do as part of the callback is to start consuming messages from the appropriate queue. Similarly, when the controller deallocates a partitions from a consumer, it fires onBecomeOfflineFromOnline for the same partition.
As a part of this transition, the consumer will stop consuming from a that queue.

```
@Transition(to = "ONLINE", from = "OFFLINE")
public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
  LOG.debug(_consumerId + " becomes ONLINE from OFFLINE for " + _partition);
  if (_thread == null) {
    LOG.debug("Starting ConsumerThread for " + _partition + "...");
    _thread = new ConsumerThread(_partition, _mqServer, _consumerId);
    _thread.start();
    LOG.debug("Starting ConsumerThread for " + _partition + " done");

  }
}

@Transition(to = "OFFLINE", from = "ONLINE")
public void onBecomeOfflineFromOnline(Message message, NotificationContext context)
    throws InterruptedException {
  LOG.debug(_consumerId + " becomes OFFLINE from ONLINE for " + _partition);
  if (_thread != null) {
    LOG.debug("Stopping " + _consumerId + " for " + _partition + "...");
    _thread.interrupt();
    _thread.join(2000);
    _thread = null;
    LOG.debug("Stopping " +  _consumerId + " for " + _partition + " done");
  }
}
```
