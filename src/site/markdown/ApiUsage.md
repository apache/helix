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


# Create an instance of Manager
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
                                                      
#Setting up a cluster
Initial setup of a cluster, involves creating appropriate znodes in the cluster. 
```
    //Create setuptool instance
    ClusterSetupTool setupTool = new ClusterSetupTool(zkConnectString);
    //Create cluster namespace in zookeeper
    setupTool.addCluster(clusterName, true);
    //Add six Participant instances, each instance must have a unique id. host:port is the standard convention
    String instances[] = new String[6];
    for (int i = 0; i < storageInstanceInfoArray.length; i++)
    {
      instance[i] = "localhost:" + (8900 + i);
    }
    setupTool.addInstancesToCluster(clusterName, instances);
    //add the resource with 10 partitions to the cluster. Using MasterSlave state model. 
    //See the section on how to configure a application specific state model
    setupTool.addResourceToCluster(clusterName, "TestDB", 10, "MasterSlave");
    //This will do the assignment of partitions to instances. Assignment algorithm is based on consistent hashing and RUSH. 
    //See how to do custom partition assignment
    setupTool.rebalanceResource(clusterName, "TestDB", 3);
```

## Participant
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

## Controller Code
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
This above snippet shows how the controller is started. Helix comes with commands to start the controller.  
```
cd helix
mvn clean install -Dmaven.test.skip=true
cd helix-core/target/helix-core-pkg/bin
chmod +x *
./run-helix-controller --zkSvr <ZookeeperServerAddress(Required)>  --cluster <Cluster name (Required)>
```

## Spectator Code
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
##  Helix Admin operations
Helix provides multiple ways to administer the cluster. It has a command line interface and also a REST interface.

```
cd helix
mvn clean install -Dmaven.test.skip=true
cd helix-core/target/helix-core-pkg/bin
chmod +x *
./helix-admin --help
    --activateCluster <clusterName grandCluster true/false>                                             Enable/disable a cluster in distributed controller mode
    --addAlert <clusterName alertName>                                                                  Add an alert
    --addCluster <clusterName>                                                                          Add a new cluster
    --addIdealState <clusterName reourceName <filename>>                                                Add a State model to a cluster
    --addNode <clusterName InstanceAddress(host:port)>                                                  Add a new Instance to a cluster
    --addResource <clusterName resourceName partitionNum stateModelRef <-mode modeValue>>               Add a resource to a cluster
    --addResourceProperty <clusterName resourceName propertyName propertyValue>                         Add a resource property
    --addStat <clusterName statName>                                                                    Add a persistent stat
    --addStateModelDef <clusterName <filename>>                                                         Add a State model to a cluster
    --bucketSize <Size of a bucket for a resource>                                                      Specify size of a bucket, used with addResourceGroup command
    --dropAlert <clusterName alertName>                                                                 Drop an alert
    --dropCluster <clusterName>                                                                         Delete a cluster
    --dropNode <clusterName InstanceAddress(host:port)>                                                 Drop an existing Instance from a cluster
    --dropResource <clusterName resourceName>                                                           Drop an existing resource from a cluster
    --dropStat <clusterName statName>                                                                   Drop a persistent stat
    --enableCluster <clusterName true/false>                                                            pause/resume the controller of a cluster
    --enableInstance <clusterName InstanceName true/false>                                              Enable/disable a Instance
    --enablePartition <clusterName instanceName resourceName partitionName true/false>                  Enable/disable a partition
    --expandCluster <clusterName>                                                                       Expand a cluster and all the resources
    --expandResource <clusterName resourceName>                                                         Expand resource to additional nodes
    --getConfig <ConfigScope(e.g. CLUSTER=cluster,RESOURCE=rc,...) KeySet(e.g. k1,k2,...)>              Get a config
    --help                                                                                              Prints command-line options info
    --key <Resource key prefix>                                                                         Specify resource key prefix, used with rebalance command
    --listClusterInfo <clusterName>                                                                     Query info of a cluster
    --listClusters                                                                                      List existing clusters
    --listInstanceInfo <clusterName InstanceName>                                                       Query info of a Instance in a cluster
    --listInstances <clusterName>                                                                       List Instances in a cluster
    --listPartitionInfo <clusterName resourceName partitionName>                                        Query info of a partition
    --listResourceInfo <clusterName resourceName>                                                       Query info of a resource
    --listResources <clusterName>                                                                       List resources hosted in a cluster
    --listStateModel <clusterName stateModelName>                                                       Query info of a state model in a cluster
    --listStateModels <clusterName>                                                                     Query info of state models in a cluster
    --mode <IdealState mode>                                                                            Specify resource mode, used with addResourceGroup command
    --rebalance <clusterName resourceName replicas>                                                     Rebalance a resource in a cluster
    --removeResourceProperty <clusterName resourceName propertyName>                                    Remove a resource property
    --resetPartition <clusterName instanceName resourceName partitionName>                              Reset a partition in error state
    --setConfig <ConfigScope(e.g. CLUSTER=cluster,RESOURCE=rc,...) KeyValueMap(e.g. k1=v1,k2=v2,...)>   Set a config
    --swapInstance <clusterName oldInstance newInstance>                                                Swap an old instance from a cluster with a new instance
    --zkSvr <ZookeeperServerAddress(Required)>                                                          Provide zookeeper address
```

## Idealstate modes and configuration


 * AUTO mode: ideal state is pre-generated using consistent hashing 
 `setupTool.addResourceToCluster(clusterName, resourceName, partitionNumber, "MasterSlave")`
 `setupTool.rebalanceStorageCluster(clusterName, resourceName, replicas)`
 * AUTO_REBALANCE mode: ideal state is generated dynamically by cluster manager
 `setupTool.addResourceToCluster(clusterName, resourceName, partitionNumber, "MasterSlave", "AUTO_REBALANCE)`
 `setupTool.rebalanceStorageCluster(clusterName, resourceName, replicas)`
 * CUSTOMIZED mode: ideal state is pre-generated from a JSON format file
 `setupTool.addIdealState(clusterName, resourceName, idealStateJsonFile)`

## Configuring state model

```
StateModelConfigGenerator generator = new StateModelConfigGenerator();
ZnRecord stateModelConfig = generator.generateConfigForOnlineOffline();
StateModelDefinition stateModelDef = new StateModelDefinition(stateModelConfig);
ClusterSetup setupTool = new ClusterSetup(zkConnectString);
setupTool.addStateModelDef(cluster,stateModelName,stateModelDef);
```
See StateModelConfigGenerator to get more info on creating custom state model.

## Messaging Api usage

See BootstrapProcess.java in examples package to see how Participants can exchange messages with each other.

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
      recipientCriteria.setInstanceName("*");
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
For more details on MessagingService see ClusterMessagingService



