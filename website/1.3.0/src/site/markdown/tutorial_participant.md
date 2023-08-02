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
  <title>Tutorial - Participant</title>
</head>

## [Helix Tutorial](./Tutorial.html): Participant

In this chapter, we\'ll learn how to implement a __Participant__, which is a primary functional component of a distributed system.


### Start a Connection

The Helix manager is a common component that connects each system component with the controller.

It requires the following parameters:

* clusterName: A logical name to represent the group of nodes
* instanceName: A logical name of the process creating the manager instance. Generally this is host:port
* instanceType: Type of the process. This can be one of the following types, in this case, use PARTICIPANT
    * CONTROLLER: Process that controls the cluster, any number of controllers can be started but only one will be active at any given time
    * PARTICIPANT: Process that performs the actual task in the distributed system
    * SPECTATOR: Process that observes the changes in the cluster
    * ADMIN: To carry out system admin actions
* zkConnectString: Connection string to ZooKeeper. This is of the form host1:port1,host2:port2,host3:port3

After the Helix manager instance is created, the only thing that needs to be registered is the state model factory.
The methods of the state model will be called when controller sends transitions to the participant.  In this example, we'll use the OnlineOffline factory.  Other options include:

* MasterSlaveStateModelFactory
* LeaderStandbyStateModelFactory
* BootstrapHandler


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

### Example State Model Factory

Helix doesn\'t know what it means to change from OFFLINE\-\-\>ONLINE or ONLINE\-\-\>OFFLINE.  The following code snippet shows where you insert your system logic for these two state transitions.

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
      System.out.println("OnlineOfflineStateModel.onBecomeOnlineFromOffline()");

      ////////////////////////////////////////////////////////////////////////////////////////////////
      // Application logic to handle transition                                                     //
      // For example, you might start a service, run initialization, etc                            //
      ////////////////////////////////////////////////////////////////////////////////////////////////
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message,
        NotificationContext context) {
      System.out.println("OnlineOfflineStateModel.onBecomeOfflineFromOnline()");

      ////////////////////////////////////////////////////////////////////////////////////////////////
      // Application logic to handle transition                                                     //
      // For example, you might shutdown a service, log this event, or change monitoring settings   //
      ////////////////////////////////////////////////////////////////////////////////////////////////
    }
  }
}
```
