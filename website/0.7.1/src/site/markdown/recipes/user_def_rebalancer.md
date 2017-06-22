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
Lock Manager with a User-Defined Rebalancer
-------------------------------------------
Helix is able to compute node preferences and state assignments automatically using general-purpose algorithms. In many cases, a distributed system implementer may choose to instead define a customized approach to computing the location of replicas, the state mapping, or both in response to the addition or removal of participants. The following is an implementation of the [Distributed Lock Manager](./lock_manager.html) that includes a user-defined rebalancer.

### Define the Cluster and Resource

The YAML file below fully defines the cluster and the locks. A lock can be in one of two states: locked and unlocked. Transitions can happen in either direction, and the locked is preferred. A resource in this example is the entire collection of locks to distribute. A partition is mapped to a lock; in this case that means there are 12 locks. These 12 locks will be distributed across 3 nodes. The constraints indicate that only one replica of a lock can be in the locked state at any given time. These locks can each only have a single holder, defined by a replica count of 1.

Notice the rebalancer section of the definition. The mode is set to USER_DEFINED and the class name refers to the plugged-in rebalancer implementation that inherits from [HelixRebalancer](http://helix.apache.org/javadocs/0.7.1/reference/org/apache/helix/controller/rebalancer/HelixRebalancer.html). This implementation is called whenever the state of the cluster changes, as is the case when participants are added or removed from the system.

Location: `helix/recipes/user-defined-rebalancer/src/main/resources/lock-manager-config.yaml`

```
clusterName: lock-manager-custom-rebalancer # unique name for the cluster
resources:
  - name: lock-group # unique resource name
    rebalancer: # we will provide our own rebalancer
      mode: USER_DEFINED
      class: org.apache.helix.userdefinedrebalancer.LockManagerRebalancer
    partitions:
      count: 12 # number of locks
      replicas: 1 # number of simultaneous holders for each lock
    stateModel:
      name: lock-unlock # unique model name
      states: [LOCKED, RELEASED, DROPPED] # the list of possible states
      transitions: # the list of possible transitions
        - name: Unlock
          from: LOCKED
          to: RELEASED
        - name: Lock
          from: RELEASED
          to: LOCKED
        - name: DropLock
          from: LOCKED
          to: DROPPED
        - name: DropUnlock
          from: RELEASED
          to: DROPPED
        - name: Undrop
          from: DROPPED
          to: RELEASED
      initialState: RELEASED
    constraints:
      state:
        counts: # maximum number of replicas of a partition that can be in each state
          - name: LOCKED
            count: "1"
          - name: RELEASED
            count: "-1"
          - name: DROPPED
            count: "-1"
        priorityList: [LOCKED, RELEASED, DROPPED] # states in order of priority
      transition: # transitions priority to enforce order that transitions occur
        priorityList: [Unlock, Lock, Undrop, DropUnlock, DropLock]
participants: # list of nodes that can acquire locks
  - name: localhost_12001
    host: localhost
    port: 12001
  - name: localhost_12002
    host: localhost
    port: 12002
  - name: localhost_12003
    host: localhost
    port: 12003
```

Then, Helix\'s YAMLClusterSetup tool can read in the configuration and bootstrap the cluster immediately:

```
YAMLClusterSetup setup = new YAMLClusterSetup(zkAddress);
InputStream input =
    Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("lock-manager-config.yaml");
YAMLClusterSetup.YAMLClusterConfig config = setup.setupCluster(input);
```

### Write a Rebalancer
Below is a full implementation of a rebalancer that extends [HelixRebalancer](http://helix.apache.org/javadocs/0.7.1/reference/org/apache/helix/controller/rebalancer/HelixRebalancer.html). In this case, it simply throws out the previous resource assignment, computes the target node for as many partition replicas as can hold a lock in the LOCKED state (in this example, one), and assigns them the LOCKED state (which is at the head of the state preference list). Clearly a more robust implementation would likely examine the current ideal state to maintain current assignments, and the full state list to handle models more complicated than this one. However, for a simple lock holder implementation, this is sufficient.

Location: `helix/recipes/user-rebalanced-lock-manager/src/main/java/org/apache/helix/userdefinedrebalancer/LockManagerRebalancer.java`

```
@Override
public ResourceAssignment computeResourceMapping(IdealState idealState,
    RebalancerConfig rebalancerConfig, ResourceAssignment prevAssignment, Cluster cluster,
    ResourceCurrentState currentState) {
  // Initialize an empty mapping of locks to participants
  ResourceAssignment assignment = new ResourceAssignment(idealState.getResourceId());

  // Get the list of live participants in the cluster
  List<ParticipantId> liveParticipants =
      new ArrayList<ParticipantId>(cluster.getLiveParticipantMap().keySet());

  // Get the state model (should be a simple lock/unlock model) and the highest-priority state
  StateModelDefId stateModelDefId = idealState.getStateModelDefId();
  StateModelDefinition stateModelDef = cluster.getStateModelMap().get(stateModelDefId);
  if (stateModelDef.getStatesPriorityList().size() < 1) {
    LOG.error("Invalid state model definition. There should be at least one state.");
    return assignment;
  }
  State lockState = stateModelDef.getTypedStatesPriorityList().get(0);

  // Count the number of participants allowed to lock each lock
  String stateCount = stateModelDef.getNumParticipantsPerState(lockState);
  int lockHolders = 0;
  try {
    // a numeric value is a custom-specified number of participants allowed to lock the lock
    lockHolders = Integer.parseInt(stateCount);
  } catch (NumberFormatException e) {
    LOG.error("Invalid state model definition. The lock state does not have a valid count");
    return assignment;
  }

  // Fairly assign the lock state to the participants using a simple mod-based sequential
  // assignment. For instance, if each lock can be held by 3 participants, lock 0 would be held
  // by participants (0, 1, 2), lock 1 would be held by (1, 2, 3), and so on, wrapping around the
  // number of participants as necessary.
  // This assumes a simple lock-unlock model where the only state of interest is which nodes have
  // acquired each lock.
  int i = 0;
  for (PartitionId partition : idealState.getPartitionIdSet()) {
    Map<ParticipantId, State> replicaMap = new HashMap<ParticipantId, State>();
    for (int j = i; j < i + lockHolders; j++) {
      int participantIndex = j % liveParticipants.size();
      ParticipantId participant = liveParticipants.get(participantIndex);
      // enforce that a participant can only have one instance of a given lock
      if (!replicaMap.containsKey(participant)) {
        replicaMap.put(participant, lockState);
      }
    }
    assignment.addReplicaMap(partition, replicaMap);
    i++;
  }
  return assignment;
}
```

### Start up the Participants
Here is a lock class based on the newly defined lock-unlock transition handler so that the participant can receive callbacks on state transitions.

Location: `helix/recipes/user-rebalanced-lock-manager/src/main/java/org/apache/helix/userdefinedrebalancer/Lock.java`

```
public class Lock extends TransitionHandler {
  private String lockName;

  public Lock(String lockName) {
    this.lockName = lockName;
  }

  @Transition(from = "RELEASED", to = "LOCKED")
  public void lock(Message m, NotificationContext context) {
    System.out.println(context.getManager().getInstanceName() + " acquired lock:" + lockName);
  }

  @Transition(from = "LOCKED", to = "RELEASED")
  public void release(Message m, NotificationContext context) {
    System.out.println(context.getManager().getInstanceName() + " releasing lock:" + lockName);
  }
}
```

Here is the factory to make the Lock class accessible.

Location: `helix/recipes/user-rebalanced-lock-manager/src/main/java/org/apache/helix/userdefinedrebalancer/LockFactory.java`

```
public class LockFactory extends StateTransitionHandlerFactory<Lock> {
  @Override
  public Lock createNewStateModel(String lockName) {
    return new Lock(lockName);
  }
}
```

Finally, here is the factory registration and the start of the participant:

```
participantManager =
    HelixManagerFactory.getZKHelixManager(clusterName, participantName, InstanceType.PARTICIPANT,
        zkAddress);
participantManager.getStateMachineEngine().registerStateModelFactory(stateModelName,
    new LockFactory());
participantManager.connect();
```

### Start up the Controller

```
controllerManager =
    HelixControllerMain.startHelixController(zkAddress, config.clusterName, "controller",
        HelixControllerMain.STANDALONE);
```

### Try It Out

```
git clone https://git-wip-us.apache.org/repos/asf/helix.git
cd helix
git checkout tags/helix-0.7.1
mvn clean install package -DskipTests
cd recipes/user-defined-rebalancer/target/user-defined-rebalancer-pkg/bin
chmod +x *
./lock-manager-demo.sh
```

#### Output

```
./lock-manager-demo
STARTING localhost_12002
STARTING localhost_12001
STARTING localhost_12003
STARTED localhost_12001
STARTED localhost_12003
STARTED localhost_12002
localhost_12003 acquired lock:lock-group_4
localhost_12002 acquired lock:lock-group_8
localhost_12001 acquired lock:lock-group_10
localhost_12001 acquired lock:lock-group_3
localhost_12001 acquired lock:lock-group_6
localhost_12003 acquired lock:lock-group_0
localhost_12002 acquired lock:lock-group_5
localhost_12001 acquired lock:lock-group_9
localhost_12002 acquired lock:lock-group_2
localhost_12003 acquired lock:lock-group_7
localhost_12003 acquired lock:lock-group_11
localhost_12002 acquired lock:lock-group_1
lockName  acquired By
======================================
lock-group_0  localhost_12003
lock-group_1  localhost_12002
lock-group_10 localhost_12001
lock-group_11 localhost_12003
lock-group_2  localhost_12002
lock-group_3  localhost_12001
lock-group_4  localhost_12003
lock-group_5  localhost_12002
lock-group_6  localhost_12001
lock-group_7  localhost_12003
lock-group_8  localhost_12002
lock-group_9  localhost_12001
Stopping the first participant
localhost_12001 Interrupted
localhost_12002 acquired lock:lock-group_3
localhost_12003 acquired lock:lock-group_6
localhost_12003 acquired lock:lock-group_10
localhost_12002 acquired lock:lock-group_9
lockName  acquired By
======================================
lock-group_0  localhost_12003
lock-group_1  localhost_12002
lock-group_10 localhost_12003
lock-group_11 localhost_12003
lock-group_2  localhost_12002
lock-group_3  localhost_12002
lock-group_4  localhost_12003
lock-group_5  localhost_12002
lock-group_6  localhost_12003
lock-group_7  localhost_12003
lock-group_8  localhost_12002
lock-group_9  localhost_12002
```

Notice that the lock assignment directly follows the assignment generated by the user-defined rebalancer both initially and after a participant is removed from the system.
