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
Distributed lock manager
------------------------
Distributed locks are used to synchronize accesses shared resources. Most applications use Zookeeper to model the distributed locks. 

The simplest way to model a lock using zookeeper is (See Zookeeper leader recipe for an exact and more advanced solution)

* Each process tries to create an emphemeral node.
* If can successfully create it then, it acquires the lock
* Else it will watch on the znode and try to acquire the lock again if the current lock holder disappears 

This is good enough if there is only one lock. But in practice, an application will need many such locks. Distributing and managing the locks among difference process becomes challenging. Extending such a solution to many locks will result in

* Uneven distribution of locks among nodes, the node that starts first will acquire all the lock. Nodes that start later will be idle.
* When a node fails, how the locks will be distributed among remaining nodes is not predicable. 
* When new nodes are added the current nodes dont relinquish the locks so that new nodes can acquire some locks

In other words we want a system to satisfy the following requirements.

* Distribute locks evenly among all nodes to get better hardware utilization
* If a node fails, the locks that were acquired by that node should be evenly distributed among other nodes
* If nodes are added, locks must be evenly re-distributed among nodes.

Helix provides a simple and elegant solution to this problem. Simply specify the number of locks and Helix will ensure that above constraints are satisfied. 

To quickly see this working run the lock-manager-demo script where 12 locks are evenly distributed among three nodes, and when a node fails, the locks get re-distributed among remaining two nodes. Note that Helix does not re-shuffle the locks completely, instead it simply distributes the locks relinquished by dead node among 2 remaining nodes evenly.

----------------------------------------------------------------------------------------

#### Short version
 This version starts multiple threads with in same process to simulate a multi node deployment. Try the long version to get a better idea of how it works.
 
```
git clone https://git-wip-us.apache.org/repos/asf/helix.git
cd helix
mvn clean install package -DskipTests
cd recipes/distributed-lock-manager/target/distributed-lock-manager-pkg/bin
chmod +x *
./lock-manager-demo
```

##### Output

```
./lock-manager-demo 
STARTING localhost_12000
STARTING localhost_12002
STARTING localhost_12001
STARTED localhost_12000
STARTED localhost_12002
STARTED localhost_12001
localhost_12001 acquired lock:lock-group_3
localhost_12000 acquired lock:lock-group_8
localhost_12001 acquired lock:lock-group_2
localhost_12001 acquired lock:lock-group_4
localhost_12002 acquired lock:lock-group_1
localhost_12002 acquired lock:lock-group_10
localhost_12000 acquired lock:lock-group_7
localhost_12001 acquired lock:lock-group_5
localhost_12002 acquired lock:lock-group_11
localhost_12000 acquired lock:lock-group_6
localhost_12002 acquired lock:lock-group_0
localhost_12000 acquired lock:lock-group_9
lockName    acquired By
======================================
lock-group_0    localhost_12002
lock-group_1    localhost_12002
lock-group_10    localhost_12002
lock-group_11    localhost_12002
lock-group_2    localhost_12001
lock-group_3    localhost_12001
lock-group_4    localhost_12001
lock-group_5    localhost_12001
lock-group_6    localhost_12000
lock-group_7    localhost_12000
lock-group_8    localhost_12000
lock-group_9    localhost_12000
Stopping localhost_12000
localhost_12000Interrupted
localhost_12001 acquired lock:lock-group_9
localhost_12001 acquired lock:lock-group_8
localhost_12002 acquired lock:lock-group_6
localhost_12002 acquired lock:lock-group_7
lockName    acquired By
======================================
lock-group_0    localhost_12002
lock-group_1    localhost_12002
lock-group_10    localhost_12002
lock-group_11    localhost_12002
lock-group_2    localhost_12001
lock-group_3    localhost_12001
lock-group_4    localhost_12001
lock-group_5    localhost_12001
lock-group_6    localhost_12002
lock-group_7    localhost_12002
lock-group_8    localhost_12001
lock-group_9    localhost_12001

```

----------------------------------------------------------------------------------------

#### Long version
This provides more details on how to setup the cluster and where to plugin application code.

##### start zookeeper

```
./start-standalone-zookeeper 2199
```

##### Create a cluster

```
./helix-admin --zkSvr localhost:2199 --addCluster lock-manager-demo
```

##### Create a lock group

Create a lock group and specify the number of locks in the lock group. 

```
./helix-admin --zkSvr localhost:2199  --addResource lock-manager-demo lock-group 6 OnlineOffline --mode FULL_AUTO
```

##### Start the nodes

Create a Lock class that handles the callbacks. 

```

public class Lock extends StateModel
{
  private String lockName;

  public Lock(String lockName)
  {
    this.lockName = lockName;
  }

  public void lock(Message m, NotificationContext context)
  {
    System.out.println(" acquired lock:"+ lockName );
  }

  public void release(Message m, NotificationContext context)
  {
    System.out.println(" releasing lock:"+ lockName );
  }

}

```

LockFactory that creates the lock
 
```
public class LockFactory extends StateModelFactory<Lock>{
    
    /* Instantiates the lock handler, one per lockName*/
    public Lock create(String lockName)
    {
        return new Lock(lockName);
    }   
}
```

At node start up, simply join the cluster and helix will invoke the appropriate call backs on Lock instance. One can start any number of nodes and Helix detects that a new node has joined the cluster and re-distributes the locks automatically.

```
public class LockProcess{

  public static void main(String args){
    String zkAddress= "localhost:2199";
    String clusterName = "lock-manager-demo";
    //Give a unique id to each process, most commonly used format hostname_port
    String instanceName ="localhost_12000";
    ZKHelixAdmin helixAdmin = new ZKHelixAdmin(zkAddress);
    //configure the instance and provide some metadata 
    InstanceConfig config = new InstanceConfig(instanceName);
    config.setHostName("localhost");
    config.setPort("12000");
    admin.addInstance(clusterName, config);
    //join the cluster
    HelixManager manager;
    manager = HelixManagerFactory.getHelixManager(clusterName,
                                                  instanceName,
                                                  InstanceType.PARTICIPANT,
                                                  zkAddress);
    manager.getStateMachineEngine().registerStateModelFactory("OnlineOffline", modelFactory);
    manager.connect();
    Thread.currentThread.join();
    }

}
```

##### Start the controller

Controller can be started either as a separate process or can be embedded within each node process

###### Separate process
This is recommended when number of nodes in the cluster >100. For fault tolerance, you can run multiple controllers on different boxes.

```
./run-helix-controller --zkSvr localhost:2199 --cluster mycluster 2>&1 > /tmp/controller.log &
```

###### Embedded within the node process
This is recommended when the number of nodes in the cluster is less than 100. To start a controller from each process, simply add the following lines to MyClass

```
public class LockProcess{

  public static void main(String args){
    String zkAddress= "localhost:2199";
    String clusterName = "lock-manager-demo";
    .
    .
    manager.connect();
    HelixManager controller;
    controller = HelixControllerMain.startHelixController(zkAddress, 
                                                          clusterName,
                                                          "controller", 
                                                          HelixControllerMain.STANDALONE);
    Thread.currentThread.join();
  }
}
```

----------------------------------------------------------------------------------------





