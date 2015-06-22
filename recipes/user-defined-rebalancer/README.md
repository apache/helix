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
Distributed lock manager with a user-defined rebalancer and YAML configuration
------------------------------------------------------------------------------
This recipe is a second take on the distributed lock manager example with two key differences
  * Instead of specifying the cluster using the HelixAdmin Java API, a YAML file indicates the cluster, its resources, and its participants. This is a simplified way to bootstrap cluster creation with a compact, logical hierarchy.
  * The rebalancing process (i.e. the algorithm that uses the cluster state to determine an assignment of locks to participants) is specified in a class defined by the recipe itself, completely independent of Helix.

For additional background and motivation, see the distributed-lock-manager recipe.

### YAML Cluster Setup
The YAML configuration below specifies a state model for a lock in which it can be locked and unlocked. At most one participant can hold the lock at any time, and there are 12 locks to distribute across 4 participants.

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

### User-Defined Rebalancer
The implementation of the Rebalancer interface is quite simple. It assumes a Lock/Unlock model where the lock state has highest priority. It uses a mod-based approach to fairly assign locks to participants so that no participant holds more than one instance of a lock, and each lock is only assigned to as many participants as can hold the same lock simultaneously. In the configuration above, only one participant can hold a given lock in the locked state.

The result is a ResourceMapping, which maps each lock to its holder and its lock state. In Helix terminology, the lock manager is the resource, a lock is a partition, its holder is a participant, and the lock state is the current state of the lock based on one of the pre-defined states in the state model.

```
@Override
public ResourceAssignment computeResourceMapping(Resource resource, IdealState currentIdealState,
    CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
  // Initialize an empty mapping of locks to participants
  ResourceAssignment assignment = new ResourceAssignment(resource.getResourceName());

  // Get the list of live participants in the cluster
  List<String> liveParticipants = new ArrayList<String>(clusterData.getLiveInstances().keySet());

  // Get the state model (should be a simple lock/unlock model) and the highest-priority state
  String stateModelName = currentIdealState.getStateModelDefRef();
  StateModelDefinition stateModelDef = clusterData.getStateModelDef(stateModelName);
  if (stateModelDef.getStatesPriorityList().size() < 1) {
    LOG.error("Invalid state model definition. There should be at least one state.");
    return assignment;
  }
  String lockState = stateModelDef.getStatesPriorityList().get(0);

  // Count the number of participants allowed to lock each lock
  String stateCount = stateModelDef.getNumInstancesPerState(lockState);
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
  for (Partition partition : resource.getPartitions()) {
    Map<String, String> replicaMap = new HashMap<String, String>();
    for (int j = i; j < i + lockHolders; j++) {
      int participantIndex = j % liveParticipants.size();
      String participant = liveParticipants.get(participantIndex);
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
----------------------------------------------------------------------------------------

#### In Action

##### Specifying a Lock StateModel
In our configuration file, we indicated a special state model with two key states: LOCKED and RELEASED. Thus, we need to provide for the participant a subclass of StateModel that can respond to transitions between those states.

```
public class Lock extends StateModel {
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

##### Loading the configuration file
We include a YAML file parser that will set up the cluster according to the specifications of the file. Here is the code that this example uses to set up the cluster:

```
YAMLClusterSetup setup = new YAMLClusterSetup(zkAddress);
InputStream input =
    Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("lock-manager-config.yaml");
YAMLClusterSetup.YAMLClusterConfig config = setup.setupCluster(input);
```
At this point, the cluster is set up and the configuration is persisted on Zookeeper. The config variable contains a snapshot of this configuration for further access.

##### Building 
```
git clone https://git-wip-us.apache.org/repos/asf/helix.git
cd helix
mvn clean install package -DskipTests
cd recipes/user-rebalanced-lock-manager/target/user-rebalanced-lock-manager-pkg/bin
chmod +x *
./lock-manager-demo
```

##### Output

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

----------------------------------------------------------------------------------------





