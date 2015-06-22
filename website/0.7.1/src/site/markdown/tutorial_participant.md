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


### Start the Helix Participant

The Helix participant class is a common component that connects each participant with the controller.

It requires the following parameters:

* clusterId: A logical ID to represent the group of nodes
* participantId: A logical ID of the process creating the manager instance. Generally this is host:port.
* zkConnectString: Connection string to Zookeeper. This is of the form host1:port1,host2:port2,host3:port3.

After the Helix participant instance is created, only thing that needs to be registered is the transition handler factory.
The methods of the TransitionHandler will be called when controller sends transitions to the Participant.  In this example, we'll use the OnlineOffline factory.  Other options include:

* MasterSlaveStateModelFactory
* LeaderStandbyStateModelFactory
* BootstrapHandler
* _An application-defined state transition handler factory_


```
HelixConnection connection = new ZKHelixConnection(zkConnectString);
HelixParticipant participant = connection.createParticipant(clusterId, participantId);
StateMachineEngine stateMach = participant.getStateMachineEngine();

// create a state transition handler factory that returns a transition handler object for each partition.
StateTransitionHandlerFactory<OnlineOfflineTransitionHandler> transitionHandlerFactory = new OnlineOfflineHandlerFactory();
stateMach.registerStateModelFactory(stateModelType, transitionHandlerFactory);
participant.start();
```

Helix doesn\'t know what it means to change from OFFLINE\-\-\>ONLINE or ONLINE\-\-\>OFFLINE.  The following code snippet shows where you insert your system logic for these two state transitions.

```
public class OnlineOfflineHandlerFactory extends StateTransitionHandlerFactory<OnlineOfflineTransitionHandler> {
  @Override
  public OnlineOfflineTransitionHandler createStateTransitionHandler(PartitionId partitionId) {
    OnlineOfflineTransitionHandler transitionhandler = new OnlineOfflineTransitionHandler();
    return transitionHandler;
  }
}

@StateModelInfo(states = "{'OFFLINE','ONLINE'}", initialState = "OFFINE")
public static class OnlineOfflineTransitionHandler extends TransitionHandler {
  @Transition(from = "OFFLINE", to = "ONLINE")
  public void onBecomeOnlineFromOffline(Message message,
      NotificationContext context) {

    System.out.println("OnlineOfflineTransitionHandler.onBecomeOnlineFromOffline()");

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Application logic to handle transition                                                     //
    // For example, you might start a service, run initialization, etc                            //
    ////////////////////////////////////////////////////////////////////////////////////////////////
  }

  @Transition(from = "ONLINE", to = "OFFLINE")
  public void onBecomeOfflineFromOnline(Message message,
      NotificationContext context) {
    System.out.println("OnlineOfflineTransitionHandler.onBecomeOfflineFromOnline()");

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Application logic to handle transition                                                     //
    // For example, you might shutdown a service, log this event, or change monitoring settings   //
    ////////////////////////////////////////////////////////////////////////////////////////////////
  }
}
```

