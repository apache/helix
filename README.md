WHAT IS HELIX
--------------
Helix is a generic cluster management framework used for automatic management of partitioned, replicated and distributed resources hosted on a group of nodes( cluster). Helix provides the following features 

1. Automatic assignment of resource/partition to nodes
2. Node Failure detection and recovery
3. Dynamic addition of Resources 
4. Dynamic Addition of nodes to the cluster
5. Pluggable distributed state machine to manage the state of a resource via state transitions
6. Automatic Load balancing and throttling of transitions 

-----

A resource (for eg a database, lucene index or any task) in general is partitioned, replicated and distributed among the nodes in the cluster. 

Each partition of a resource can have a state associated with it. Here are some common state models used

1. Master, Slave
2. Online, Offline
3. Leader, Standby.

Helix manages the state of a resource by supporting a pluggable distributed state machine. One can define the state machine table along with the constraints for each state. For example in case of a MasterSlave state model one can specify the state machine as

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

Helix will automatically try to maintain 1 Master and 2 Slaves by initiating appropriate state transitions in the cluster. 

Each transition results in a partition moving from its CURRENT state state to an NEW state. These transitions are triggered on changes in the cluster state like 

* Node start up
* Node soft and hard failures 
* Addition of resources
* Addition of nodes

---------


With these features, Helix framework can be used to build distributed, scalable, elastic and fault tolerant systems by configuring the distributed state machine and its constraints based on application requirement. Application has to provide the implementation for handling state transitions appropriately. Example 

<pre><code>
MasterSlaveStateModel extends HelixStateModel {

  void onOfflineToSlave(Message m, NotificationContext context){
    print("Transitioning from Offline to Slave for database:"+ m.resourceGroup + " and partition:"+ m.resourceKey);
  }
  void onSlaveToMaster(Message m, NotificationContext context){
    print("Transitioning from Slave to Master for database:"+ m.resourceGroup + " and partition:"+ m.resourceKey);
  }
  void onMasterToSlave(Message m, NotificationContext context){
    print("Transitioning from Master to Slave for database:"+ m.resourceGroup + " and partition:"+ m.resourceKey);
  }
  void onSlaveToOffline(Message m, NotificationContext context){
    print("Transitioning from Slave to Offline for database:"+ m.resourceGroup + " and partition:"+ m.resourceKey);
  }

}

</code></pre>

The only thing users have to now worry about is handling the transitions appropriately.

Helix uses Zookeeper for maintaining the cluster state and change notification.

----------------

Quickstart
---------------

Install/Start zookeeper
-----------------------


Zookeeper can be started in standalone mode or replicated mode.

More can info is available at http://zookeeper.apache.org/doc/r3.3.3/zookeeperStarted.html
and  http://zookeeper.apache.org/doc/trunk/zookeeperAdmin.html#sc_zkMulitServerSetup

In this example, we will start zookeeper in local mode.

BUILD Helix
-----------

    git clone git@github.com:linkedin/helix.git
    cd helix-core
    mvn install package appassembler:assemble -Dmaven.test.skip=true 
    cd target/helix-core-pkg/bin //This folder contains all the scripts used in following sections
    chmod \+x *

Cluster setup
-------------
cluster-admin tool is used for cluster administration tasks. apart from command line interface Helix supports a REST interface.

zookeeper_address is of the format host:port e.g localhost:2199 for standalone or host1:port,host2:port for multi node.

In the following section we will see how one can set up a mock cluster with 

* mycluster as cluster name
* 3 nodes running on localhost at 12913, 12914,12915 
* One database named MyDB with 6 partitions
* Each partition will have 1 master, 2 slaves
* zookeeper running locally at localhost:2199

Steps
-----
    
#### start zookeeper locally at port 2199

    ./start-standalone-zookeeper 2199 &

#### create the cluster mycluster
    ## helix-admin --zkSvr localhost:2199 --addCluster <clustername> 
    ./helix-admin --zkSvr localhost:2199 --addCluster mycluster 

#### Create a database using MasterSlave state model. This ensures there will be one master and 
    ### helix-admin --zkSvr localhost:2199  --addResource <clustername> <resourceName> <numPartitions> <StateModelName>
    ./helix-admin --zkSvr localhost:2199  --addResource mycluster myDB 6 MasterSlave
   
#### Add nodes to the cluster, in this case we add three nodes, hostname:port is host and port on which the service will start
    ## helix-admin --zkSvr <zk_address>  --addNode <clustername> <host:port>
    ./helix-admin --zkSvr localhost:2199  --addNode mycluster localhost:12913
    ./helix-admin --zkSvr localhost:2199  --addNode mycluster localhost:12914
    ./helix-admin --zkSvr localhost:2199  --addNode mycluster localhost:12915

#### After adding nodes assign partitions to nodes. This command will distribute the partitions amongst all the nodes in the cluster. Each partition will have 3 replicas
    
     helix-admin --rebalance <clustername> <resourceName> <replication factor>
    ./helix-admin --zkSvr localhost:2199 --rebalance mycluster myDB 3

#### Start Helix Controller

    #This will start the cluster manager which will manage <mycluster>
    ./run-helix-controller --zkSvr localhost:2199 --cluster mycluster 2>&1 > /tmp/controller.log &

#### Start Example Participant

   
    ./start-helix-participant --help
    # start process 1 process corresponding to every host port added during cluster setup
    ./start-helix-participant --zkSvr localhost:2199 --cluster mycluster --host localhost --port 12913 --stateModelType MasterSlave 2>&1 > /tmp/participant_12913.log 
    ./start-helix-participant --zkSvr localhost:2199 --cluster mycluster --host localhost --port 12914 --stateModelType MasterSlave 2>&1 > /tmp/participant_12914.log
    ./start-helix-participant --zkSvr localhost:2199 --cluster mycluster --host localhost --port 12915 --stateModelType MasterSlave 2>&1 > /tmp/participant_12915.log


Inspect Cluster Data
--------------------

We can see the cluster state on zookeeper and know the partition assignment and current state of each parition.

Command line tool
#### List existing clusters
    ./helix-admin --zkSvr localhost:2199 --listClusters        
                                       
####  Query info of a cluster

    #helix-admin --zkSvr localhost:2199 --listClusterInfo <clusterName> 
    ./helix-admin --zkSvr localhost:2199 --listClusterInfo mycluster

####  List Instances in a cluster
    ## helix-admin --zkSvr localhost:2199 --listInstances <clusterName>
     ./helix-admin --zkSvr localhost:2199 --listInstances mycluster
    
#### Query info of a Instance in a cluster
    #./helix-admin --zkSvr localhost:2199 --listInstanceInfo <clusterName InstanceName>    
     ./helix-admin --zkSvr localhost:2199 --listInstanceInfo mycluster localhost_12913
     ./helix-admin --zkSvr localhost:2199 --listInstanceInfo mycluster localhost_12914
     ./helix-admin --zkSvr localhost:2199 --listInstanceInfo mycluster localhost_12915

####  List resourceGroups hosted in a cluster
    ## helix-admin --zkSvr localhost:2199 --listResources <clusterName>
    ./helix-admin --zkSvr localhost:2199 --listResources mycluster
    
####  Query info of a resource
    ## helix-admin --zkSvr localhost:2199 --listResourceInfo <clusterName resourceName>
    ./helix-admin --zkSvr localhost:2199 --listResourceInfo mycluster myDB

#### Query info about a partition   
    ## helix-admin --zkSvr localhost:2199 --listResourceInfo <clusterName partition> 
    ./helix-admin --zkSvr localhost:2199 --listResourceInfo mycluster myDB_0
   
#### List all state models in the cluster
    # helix-admin --zkSvr localhost:2199 --listStateModels <clusterName>
    ./helix-admin --zkSvr localhost:2199 --listStateModels mycluster
    
#### Query info about a state model in a cluster
    ## helix-admin --zkSvr localhost:2199 --listStateModel <clusterName stateModelName>
    ./helix-admin --zkSvr localhost:2199 --listStateModel mycluster MasterSlave

#### ZOOINSPECTOR

Use ZooInspector that comes with zookeeper to browse the data. This is a java applet ( make sure you have X windows)
To start zooinspector run the following command from <zk_install_directory>/contrib/ZooInspector/bin
      
    java -cp zookeeper-3.3.3-ZooInspector.jar:lib/jtoaster-1.0.4.jar:../../lib/log4j-1.2.15.jar:../../zookeeper-3.3.3.jar org.apache.zookeeper.inspector.ZooInspector
   

