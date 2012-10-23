Install/Start zookeeper
-----------------------

Zookeeper can be started in standalone mode or replicated mode.

More info is available at http://zookeeper.apache.org/doc/r3.3.3/zookeeperStarted.html
and http://zookeeper.apache.org/doc/trunk/zookeeperAdmin.html#sc_zkMulitServerSetup

In this example, we will start zookeeper in local mode.

BUILD Helix
-----------
Jump to Download section to skip this test.

    git clone git@github.com:linkedin/helix.git
    cd helix-core
    mvn install package appassembler:assemble -Dmaven.test.skip=true 
    cd target/helix-core-pkg/bin //This folder contains all the scripts used in following sections
    chmod \+x *

Download Helix
--------------
Instead of building the package from the code, you can download the 0.5.28 release package from [here](http://linkedin.github.com/helix/download/release-0.5.28/helix-core-pkg-0.5.28.tar.gz) 

Cluster setup
-------------
cluster-admin tool is used for cluster administration tasks. Apart from a command line interface Helix supports a REST interface as well.

zookeeper_address is of the format host:port e.g localhost:2199 for standalone or host1:port,host2:port for multi node.

In the following section we will see how one can set up a mock mycluster cluster with 

* 3 node instances running on localhost at 12913, 12914,12915 
* One database named MyDB with 6 partitions 
* Each partition will have 3 replicas with 1 master, 2 slaves
* zookeeper running locally at localhost:2199

Note that this mock cluster does not have any functionality apart from handling Helix callbacks.
 
Steps
-----
If you build the code
cd helix/helix-core/target/helix-core-pkg
If you download the release package, extract it.
cd helix-core-pkg
     
#### start zookeeper locally at port 2199

    ./start-standalone-zookeeper 2199 &

#### create the cluster mycluster
    ## helix-admin --zkSvr localhost:2199 --addCluster <clustername> 
    ./helix-admin --zkSvr localhost:2199 --addCluster mycluster 

#### Create a database with 6 partitions using MasterSlave state model. This ensures there will be one master for each partition 
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

#### Start Example Participant, This is a dummy participant where the transitions are no-ops.    
    ./start-helix-participant --help
    # start process 1 process corresponding to every host port added during cluster setup
    ./start-helix-participant --zkSvr localhost:2199 --cluster mycluster --host localhost --port 12913 --stateModelType MasterSlave 2>&1 > /tmp/participant_12913.log 
    ./start-helix-participant --zkSvr localhost:2199 --cluster mycluster --host localhost --port 12914 --stateModelType MasterSlave 2>&1 > /tmp/participant_12914.log
    ./start-helix-participant --zkSvr localhost:2199 --cluster mycluster --host localhost --port 12915 --stateModelType MasterSlave 2>&1 > /tmp/participant_12915.log


Inspect Cluster Data
--------------------

We can see the cluster state on zookeeper and know the partition assignment and current state of each partition.

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

#### List resourceGroups hosted in a cluster
    ## helix-admin --zkSvr localhost:2199 --listResources <clusterName>
    ./helix-admin --zkSvr localhost:2199 --listResources mycluster
    
#### Query info of a resource
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
To start zooinspector run the following command from <zk_install_directory>/contrib/ZooInspector
      
    java -cp zookeeper-3.3.3-ZooInspector.jar:lib/jtoaster-1.0.4.jar:../../lib/log4j-1.2.15.jar:../../zookeeper-3.3.3.jar org.apache.zookeeper.inspector.ZooInspector