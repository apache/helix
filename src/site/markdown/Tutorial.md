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

Lets walk through the steps in building a distributed system using Helix.

### Start zookeeper

This starts a zookeeper in standalone mode. For production deployment, see [Apache Zookeeper] page for instructions.

```
    ./start-standalone-zookeeper.sh 2199 &
```

### Create cluster

Creating a cluster will create appropriate znodes on zookeeper.   

```
    //Create setuptool instance
    admin = new ZKHelixAdmin(ZK_ADDRESS);
    String CLUSTER_NAME = "helix-demo";
    //Create cluster namespace in zookeeper
    admin.addCluster(clusterName);
```

OR

```
    ./helix-admin.sh --zkSvr localhost:2199 --addCluster helix-demo 
```


### Configure nodes

Add new nodes to the cluster, configure new nodes in the cluster. Each node in the cluster must be uniquely identifiable. 
Most commonly used convention is hostname:port.


```
    String CLUSTER_NAME = "helix-demo";
    int NUM_NODES = 2;
    String hosts[] = new String[]{"localhost","localhost"};
    String ports[] = new String[]{7000,7001};
    for (int i = 0; i < NUM_NODES; i++)
    {
      
      InstanceConfig instanceConfig = new InstanceConfig(hosts[i]+ "_" + ports[i]);
      instanceConfig.setHostName(hosts[i]);
      instanceConfig.setPort(ports[i]);
      instanceConfig.setInstanceEnabled(true);
      //Add additional system specific configuration if needed. These can be accessed during the node start up.
      instanceConfig.getRecord().setSimpleField("key", "value");
      admin.addInstance(CLUSTER_NAME, instanceConfig);
      
    }

```

### Configure the resource

Resource represents the actual task performed by the nodes. It can be a database, index, topic, queue or any other processing.
A Resource can be divided into many sub parts called as partitions. 


#### Define state model and constraints

For scalability and fault tolerance each partition can have one or more replicas. 
State model allows one to declare the system behavior by first enumerating the various STATES and TRANSITIONS between them.
A simple model is ONLINE-OFFLINE where ONLINE means the task is active and OFFLINE means its not active.
You can also specify how of replicas must be in each state. 
For example In a Search System, one might need more than one node serving the same index. 
Helix allows one to express this via constraints on each STATE.   

The following snippet shows how to declare the state model and constraints for MASTER-SLAVE model.

```

    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(
        STATE_MODEL_NAME);
    // Add states and their rank to indicate priority. Lower the rank higher the
    // priority
    builder.addState(MASTER, 1);
    builder.addState(SLAVE, 2);
    builder.addState(OFFLINE);
    // Set the initial state when the node starts
    builder.initialState(OFFLINE);

    // Add transitions between the states.
    builder.addTransition(OFFLINE, SLAVE);
    builder.addTransition(SLAVE, OFFLINE);
    builder.addTransition(SLAVE, MASTER);
    builder.addTransition(MASTER, SLAVE);

    // set constraints on states.
    // static constraint
    builder.upperBound(MASTER, 1);
    // dynamic constraint, R means it should be derived based on the replica,
    // this allows use different replication factor for each resource without 
    //having to define a new state model
    builder.dynamicUpperBound(SLAVE, "R");

    StateModelDefinition statemodelDefinition = builder.build();
    admin.addStateModelDef(CLUSTER_NAME, STATE_MODEL_NAME, myStateModel);
   
```



 
#### Assigning partitions to nodes

The final goal of Helix is to ensure that the constraints on the state model are satisfied. 
Helix does this by assigning a STATE to a partition and placing it on a particular node.


There are 3 assignment modes Helix can operate on

* AUTO_REBALANCE: Helix decides the placement and state of a partition.
* AUTO: Application decides the placement but Helix decides the state of a partition.
* CUSTOM: Application controls the placement and state of a partition.

For more info on the modes see the *partition placement* section on [Features](./Features.html) page.

```
    String RESOURCE_NAME="MyDB";
    int NUM_PARTITIONs=6;
    STATE_MODEL_NAME = "MasterSlave";
    String MODE = "AUTO";
    int NUM_REPLICAS = 2;
    admin.addResource(CLUSTER_NAME, RESOURCE_NAME, NUM_PARTITIONS, STATE_MODEL_NAME, MODE);
    admin.rebalance(CLUSTER_NAME, RESOURCE_NAME, NUM_REPLICAS);
```

### Starting a Helix based process

The first step of using the Helix api will be creating a Helix manager instance. 
It requires the following parameters:
 
* clusterName: A logical name to represent the group of nodes
* instanceName: A logical name of the process creating the manager instance. Generally this is host:port.
* instanceType: Type of the process. This can be one of the following types:
    * CONTROLLER: Process that controls the cluster, any number of controllers can be started but only one will be active at any given time.
    * PARTICIPANT: Process that performs the actual task in the distributed system. 
    * SPECTATOR: Process that observes the changes in the cluster.
    * ADMIN: To carry out system admin actions.
* zkConnectString: Connection string to Zookeeper. This is of the form host1:port1,host2:port2,host3:port3. 

```
      manager = HelixManagerFactory.getZKHelixManager(clusterName,
                                                      instanceName,
                                                      instanceType,
                                                      zkConnectString);
```
                                                      


### Participant
Starting up a participant is pretty straightforward. After the Helix manager instance is created, only thing that needs to be registered is the state model factory. 
The Methods on the State Model will be called when controller sends transitions to the Participant.

```
      manager = HelixManagerFactory.getZKHelixManager(clusterName,
                                                          instanceName,
                                                          InstanceType.PARTICIPANT,
                                                          zkConnectString);
     StateMachineEngine stateMach = manager.getStateMachineEngine();
     //create a stateModelFactory that returns a statemodel object for each partition. 
     stateModelFactory = new OnlineOfflineStateModelFactory();     
     stateMach.registerStateModelFactory(stateModelType, stateModelFactory);
     manager.connect();
```

```
public class OnlineOfflineStateModelFactory extends
        StateModelFactory<StateModel> {
    @Override
    public StateModel createNewStateModel(String stateUnitKey) {
        OnlineOfflineStateModel stateModel = new OnlineOfflineStateModel();
        return stateModel;
    }
    @StateModelInfo(states = "{'OFFLINE','ONLINE'}", initialState = "OFFINE")
    public static class OnlineOfflineStateModel extends StateModel {
        @Transition(from = "OFFLINE", to = "ONLINE")
        public void onBecomeOnlineFromOffline(Message message,
                NotificationContext context) {
            System.out
                    .println("OnlineOfflineStateModel.onBecomeOnlineFromOffline()");
            //Application logic to handle transition 
        }
        @Transition(from = "ONLINE", to = "OFFLINE")
        public void onBecomeOfflineFromOnline(Message message,
                NotificationContext context) {
            System.out
                        .println("OnlineOfflineStateModel.onBecomeOfflineFromOnline()");
            //Application logic to handle transition
        }
    }
}
```

### Controller Code
Controller needs to know about all changes in the cluster. Helix comes with default implementation to handle all changes in the cluster. 
If you have a need to add additional functionality, see GenericHelixController on how to configure the pipeline.


```
      manager = HelixManagerFactory.getZKHelixManager(clusterName,
                                                          instanceName,
                                                          InstanceType.CONTROLLER,
                                                          zkConnectString);
     manager.connect();
     GenericHelixController controller = new GenericHelixController();
     manager.addConfigChangeListener(controller);
     manager.addLiveInstanceChangeListener(controller);
     manager.addIdealStateChangeListener(controller);
     manager.addExternalViewChangeListener(controller);
     manager.addControllerListener(controller);
```
This above snippet shows how the controller is started. You can also start the controller using command line interface.
  
```
cd helix
mvn clean install -Dmaven.test.skip=true
cd helix-core/target/helix-core-pkg/bin
chmod +x *
./run-helix-controller.sh --zkSvr <ZookeeperServerAddress(Required)>  --cluster <Cluster name (Required)>
```
See controller deployment modes section in [Features](./Features.html) page for different ways to deploy the controller.

### Spectator Code
A spectator simply observes all cluster is notified when the state of the system changes. Helix consolidates the state of entire cluster in one Znode called ExternalView.
Helix provides a default implementation RoutingTableProvider that caches the cluster state and updates it when there is a change in the cluster

```
manager = HelixManagerFactory.getZKHelixManager(clusterName,
                                                          instanceName,
                                                          InstanceType.PARTICIPANT,
                                                          zkConnectString);
manager.connect();
RoutingTableProvider routingTableProvider = new RoutingTableProvider();
manager.addExternalViewChangeListener(routingTableProvider);

```

In order to figure out who is serving a partition, here are the apis

```
instances = routingTableProvider.getInstances("DBNAME", "PARITION_NAME", "PARTITION_STATE");
```

### Zookeeper znode layout.

See  *Helix znode layout* section in [Architecture](./Architecture.html) page.


###  Helix Admin operations

Helix provides multiple ways to administer the cluster. It has a command line interface and also a REST interface.

```
cd helix
mvn clean install -Dmaven.test.skip=true
cd helix-core/target/helix-core-pkg/bin
chmod +x *
./helix-admin.sh --help
Provide zookeeper address. Required for all commands  
   --zkSvr <ZookeeperServerAddress(Required)>       

Add a new cluster                                                          
   --addCluster <clusterName>                              

Add a new Instance to a cluster                                    
   --addNode <clusterName InstanceAddress(host:port)>                                      

Add a State model to a cluster                                     
   --addStateModelDef <clusterName <filename>>    

Add a resource to a cluster            
   --addResource <clusterName resourceName partitionNum stateModelRef <mode(AUTO_REBALANCE|AUTO|CUSTOM)>>      

Upload an IdealState(Partition to Node Mapping)                                         
   --addIdealState <clusterName resourceName <filename>>            

Delete a cluster
   --dropCluster <clusterName>                                                                         

Delete a resource
   --dropResource <clusterName resourceName>                                                           Drop an existing resource from a cluster

Drop an existing Instance from a cluster    
   --dropNode <clusterName InstanceAddress(host:port)>                    

Enable/disable the entire cluster, this will basically pause the controller which means no transitions will be trigger, but the existing node sin the cluster continue to function 
   --enableCluster <clusterName>

Enable/disable a Instance. Useful to take a faulty node out of the cluster.
   --enableInstance <clusterName InstanceName true/false>

Enable/disable a partition
   --enablePartition <clusterName instanceName resourceName partitionName true/false>


   --listClusterInfo <clusterName>                                                                     Query info of a cluster
   --listClusters                                                                                      List existing clusters
   --listInstanceInfo <clusterName InstanceName>                                                       Query info of a Instance in a cluster
   --listInstances <clusterName>                                                                       List Instances in a cluster
   --listPartitionInfo <clusterName resourceName partitionName>                                        Query info of a partition
   --listResourceInfo <clusterName resourceName>                                                       Query info of a resource
   --listResources <clusterName>                                                                       List resources hosted in a cluster
   --listStateModel <clusterName stateModelName>                                                       Query info of a state model in a cluster
   --listStateModels <clusterName>                                                                     Query info of state models in a cluster

```





