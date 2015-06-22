package org.apache.helix.integration;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.ConstraintItemBuilder;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class TestPreferenceListAsQueue extends ZkTestBase {
  private static final Logger LOG = Logger.getLogger(TestPreferenceListAsQueue.class);
  private static final int TRANSITION_TIME = 500;
  private static final int WAIT_TIME = TRANSITION_TIME + (TRANSITION_TIME / 2);
  private static final int PARALLELISM = 1;

  private List<String> _instanceList;
  private Queue<List<String>> _prefListHistory;
  private String _clusterName;
  private String _stateModel;
  private HelixAdmin _admin;
  private CountDownLatch _onlineLatch;
  private CountDownLatch _offlineLatch;

  @BeforeMethod
  public void beforeMethod() {
    _instanceList = Lists.newLinkedList();
    _prefListHistory = Lists.newLinkedList();
    _admin = _setupTool.getClusterManagementTool();

    // Create cluster
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    _clusterName = className + "_" + methodName;
    _setupTool.addCluster(_clusterName, true);
  }

  /**
   * This test ensures successful creation when the state model has OFFLINE --> deprioritized and
   * a partition-level constraint enforces parallelism
   * @throws Exception
   */
  @Test
  public void testReprioritizedWithConstraint() throws Exception {
    _stateModel = "OnlineOfflineReprioritized";

    // Add a state model with the transition to ONLINE deprioritized
    _setupTool.addStateModelDef(_clusterName, _stateModel,
        createReprioritizedStateModelDef(_stateModel));

    // Add a constraint of one transition per partition
    ConstraintItemBuilder constraintItemBuilder = new ConstraintItemBuilder();
    constraintItemBuilder
        .addConstraintAttribute(ConstraintAttribute.MESSAGE_TYPE.toString(), "STATE_TRANSITION")
        .addConstraintAttribute(ConstraintAttribute.PARTITION.toString(), ".*")
        .addConstraintAttribute(ConstraintAttribute.CONSTRAINT_VALUE.toString(),
            String.valueOf(PARALLELISM));
    _admin.setConstraint(_clusterName, ConstraintType.MESSAGE_CONSTRAINT, "constraint_1",
        constraintItemBuilder.build());
    runTest();
  }

  /**
   * This test directly embeds the parallelism per partition directly into the upper bound for the
   * ONLINE state. This does not require the controller to support partition-level constraints.
   * @throws Exception
   */
  @Test
  public void testParallelismInStateModel() throws Exception {
    _stateModel = "OnlineOfflineBounded";

    // Add a state model with the parallelism implicit
    _setupTool.addStateModelDef(_clusterName, _stateModel,
        createEnforcedParallelismStateModelDef(_stateModel, PARALLELISM));
    runTest();
  }

  private void runTest() throws Exception {
    final int NUM_PARTITIONS = 1;
    final int NUM_REPLICAS = 2;
    final int NUM_INSTANCES = 2;
    final String RESOURCE_NAME = "MyResource";
    Thread.sleep(WAIT_TIME);
    _instanceList.clear();

    // Setup instances
    String[] instanceInfoArray = {
        "localhost_1", "localhost_2"
    };
    _setupTool.addInstancesToCluster(_clusterName, instanceInfoArray);

    // Add resource
    _setupTool.addResourceToCluster(_clusterName, RESOURCE_NAME, NUM_PARTITIONS, _stateModel,
        RebalanceMode.SEMI_AUTO.toString());

    // Update resource with empty preference lists
    IdealState idealState = _admin.getResourceIdealState(_clusterName, RESOURCE_NAME);
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      String partitionName = RESOURCE_NAME + "_" + i;
      List<String> dummyPreferences = Lists.newArrayList();
      for (int j = 0; j < NUM_REPLICAS; j++) {
        // hack: need to have some dummy values in the preference list to pass validation
        dummyPreferences.add("");
      }
      idealState.getRecord().setListField(partitionName, dummyPreferences);
    }
    idealState.setReplicas(String.valueOf(NUM_REPLICAS));
    _admin.setResourceIdealState(_clusterName, RESOURCE_NAME, idealState);

    // Start some instances
    HelixManager[] participants = new HelixManager[NUM_INSTANCES];
    for (int i = 0; i < NUM_INSTANCES; i++) {
      participants[i] =
          HelixManagerFactory.getZKHelixManager(_clusterName, instanceInfoArray[i],
              InstanceType.PARTICIPANT, _zkaddr);
      participants[i].getStateMachineEngine().registerStateModelFactory(
          StateModelDefId.from(_stateModel), new PrefListTaskOnlineOfflineStateModelFactory());
      participants[i].connect();
    }

    // Start the controller
    HelixManager controller =
        HelixManagerFactory.getZKHelixManager(_clusterName, null, InstanceType.CONTROLLER, _zkaddr);
    controller.connect();

    // Disable controller immediately
    _admin.enableCluster(_clusterName, false);

    // This resource only has 1 partition
    String partitionName = RESOURCE_NAME + "_" + 0;

    // There should be no preference lists yet
    Assert.assertTrue(preferenceListIsCorrect(_admin, _clusterName, RESOURCE_NAME, partitionName,
        Arrays.asList("", "")));

    // Add one instance
    addInstanceToPreferences(participants[0].getHelixDataAccessor(),
        participants[0].getInstanceName(), RESOURCE_NAME, Arrays.asList(partitionName));
    Assert.assertTrue(preferenceListIsCorrect(_admin, _clusterName, RESOURCE_NAME, partitionName,
        Arrays.asList("localhost_1", "")));

    // Add a second instance immediately; the first one should still exist
    addInstanceToPreferences(participants[1].getHelixDataAccessor(),
        participants[1].getInstanceName(), RESOURCE_NAME, Arrays.asList(partitionName));
    Assert.assertTrue(preferenceListIsCorrect(_admin, _clusterName, RESOURCE_NAME, partitionName,
        Arrays.asList("localhost_1", "localhost_2")));

    // Add the first instance again; it should already exist
    addInstanceToPreferences(participants[0].getHelixDataAccessor(),
        participants[0].getInstanceName(), RESOURCE_NAME, Arrays.asList(partitionName));
    Assert.assertTrue(preferenceListIsCorrect(_admin, _clusterName, RESOURCE_NAME, partitionName,
        Arrays.asList("localhost_1", "localhost_2")));

    // Prepare for synchronization
    _onlineLatch = new CountDownLatch(2);
    _offlineLatch = new CountDownLatch(2);
    _prefListHistory.clear();

    // Now reenable the controller
    _admin.enableCluster(_clusterName, true);

    // Now wait for both instances to be done
    boolean countReached = _onlineLatch.await(10000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(countReached);
    List<String> top = _prefListHistory.poll();
    Assert.assertTrue(top.equals(Arrays.asList("localhost_1", ""))
        || top.equals(Arrays.asList("localhost_2", "")));
    Assert.assertEquals(_prefListHistory.poll(), Arrays.asList("", ""));

    // Wait for everything to be fully offline
    countReached = _offlineLatch.await(10000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(countReached);

    // Add back the instances in the opposite order
    _admin.enableCluster(_clusterName, false);
    addInstanceToPreferences(participants[0].getHelixDataAccessor(),
        participants[1].getInstanceName(), RESOURCE_NAME, Arrays.asList(partitionName));
    addInstanceToPreferences(participants[0].getHelixDataAccessor(),
        participants[0].getInstanceName(), RESOURCE_NAME, Arrays.asList(partitionName));
    Assert.assertTrue(preferenceListIsCorrect(_admin, _clusterName, RESOURCE_NAME, partitionName,
        Arrays.asList("localhost_2", "localhost_1")));

    // Reset the latch
    _onlineLatch = new CountDownLatch(2);
    _prefListHistory.clear();
    _admin.enableCluster(_clusterName, true);

    // Now wait both to be done again
    countReached = _onlineLatch.await(10000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(countReached);
    top = _prefListHistory.poll();
    Assert.assertTrue(top.equals(Arrays.asList("localhost_1", ""))
        || top.equals(Arrays.asList("localhost_2", "")));
    Assert.assertEquals(_prefListHistory.poll(), Arrays.asList("", ""));
    Assert.assertEquals(_instanceList.size(), 0);

    // Cleanup
    controller.disconnect();
    for (HelixManager participant : participants) {
      participant.disconnect();
    }
  }

  /**
   * Create a modified version of OnlineOffline where the transition to ONLINE is given lowest
   * priority
   * @param stateModelName
   * @return
   */
  private StateModelDefinition createReprioritizedStateModelDef(String stateModelName) {
    StateModelDefinition.Builder builder =
        new StateModelDefinition.Builder(stateModelName).addState("ONLINE", 1).addState("OFFLINE")
            .addState("DROPPED").addState("ERROR").initialState("OFFLINE")
            .addTransition("ERROR", "OFFLINE", 1).addTransition("ONLINE", "OFFLINE", 2)
            .addTransition("OFFLINE", "DROPPED", 3).addTransition("OFFLINE", "ONLINE", 4)
            .dynamicUpperBound("ONLINE", "R").upperBound("OFFLINE", -1).upperBound("DROPPED", -1)
            .upperBound("ERROR", -1);
    return builder.build();
  }

  /**
   * Create a modified version of OnlineOffline where the parallelism is enforced by the upper bound
   * of ONLINE
   * @param stateModelName
   * @param parallelism
   * @return
   */
  private StateModelDefinition createEnforcedParallelismStateModelDef(String stateModelName,
      int parallelism) {
    StateModelDefinition.Builder builder =
        new StateModelDefinition.Builder(stateModelName).addState("ONLINE", 1).addState("OFFLINE")
            .addState("DROPPED").addState("ERROR").initialState("OFFLINE")
            .addTransition("ERROR", "OFFLINE", 1).addTransition("ONLINE", "OFFLINE", 2)
            .addTransition("OFFLINE", "DROPPED", 3).addTransition("OFFLINE", "ONLINE", 4)
            .dynamicUpperBound("ONLINE", String.valueOf(PARALLELISM)).upperBound("OFFLINE", -1)
            .upperBound("DROPPED", -1).upperBound("ERROR", -1);
    return builder.build();
  }

  /**
   * Check if the provided list matches the currently persisted preference list
   * @param admin
   * @param clusterName
   * @param resourceName
   * @param partitionName
   * @param expectPreferenceList
   * @return
   */
  private boolean preferenceListIsCorrect(HelixAdmin admin, String clusterName,
      String resourceName, String partitionName, List<String> expectPreferenceList) {
    IdealState idealState = admin.getResourceIdealState(clusterName, resourceName);
    List<String> preferenceList = idealState.getPreferenceList(partitionName);
    return expectPreferenceList.equals(preferenceList);
  }

  /**
   * Update an ideal state so that partitions will have an instance removed from their preference
   * lists
   * @param accessor
   * @param instanceName
   * @param resourceName
   * @param partitionName
   */
  private void removeInstanceFromPreferences(HelixDataAccessor accessor, final String instanceName,
      final String resourceName, final String partitionName) {
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    String idealStatePath = keyBuilder.idealStates(resourceName).getPath();
    synchronized (_prefListHistory) {
      // Updater for ideal state
      final List<String> prefList = Lists.newLinkedList();
      DataUpdater<ZNRecord> idealStateUpdater = new DataUpdater<ZNRecord>() {
        @Override
        public ZNRecord update(ZNRecord currentData) {
          List<String> preferenceList = currentData.getListField(partitionName);
          int numReplicas =
              Integer.valueOf(currentData.getSimpleField(IdealStateProperty.REPLICAS.toString()));
          List<String> newPrefList =
              removeInstanceFromPreferenceList(preferenceList, instanceName, numReplicas);
          currentData.setListField(partitionName, newPrefList);
          prefList.clear();
          prefList.addAll(newPrefList);
          return currentData;
        }
      };
      List<DataUpdater<ZNRecord>> updaters = Lists.newArrayList();
      updaters.add(idealStateUpdater);
      accessor.updateChildren(Arrays.asList(idealStatePath), updaters, AccessOption.PERSISTENT);
      _prefListHistory.add(prefList);
    }
  }

  /**
   * Update an ideal state so that partitions will have a new instance at the tails of their
   * preference lists
   * @param accessor
   * @param instanceName
   * @param resourceName
   * @param partitions
   */
  private void addInstanceToPreferences(HelixDataAccessor accessor, final String instanceName,
      final String resourceName, final List<String> partitions) {
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    String idealStatePath = keyBuilder.idealStates(resourceName).getPath();
    synchronized (_prefListHistory) {
      // Updater for ideal state
      final List<String> prefList = Lists.newLinkedList();
      DataUpdater<ZNRecord> idealStateUpdater = new DataUpdater<ZNRecord>() {
        @Override
        public ZNRecord update(ZNRecord currentData) {
          for (String partitionName : partitions) {
            List<String> preferenceList = currentData.getListField(partitionName);
            int numReplicas =
                Integer.valueOf(currentData.getSimpleField(IdealStateProperty.REPLICAS.toString()));
            List<String> newPrefList =
                addInstanceToPreferenceList(preferenceList, instanceName, numReplicas);
            currentData.setListField(partitionName, newPrefList);
            prefList.clear();
            prefList.addAll(newPrefList);
          }
          return currentData;
        }
      };

      // Send update requests together
      List<DataUpdater<ZNRecord>> updaters = Lists.newArrayList();
      updaters.add(idealStateUpdater);
      accessor.updateChildren(Arrays.asList(idealStatePath), updaters, AccessOption.PERSISTENT);
      _prefListHistory.add(prefList);
    }
  }

  /**
   * Add the instance to the preference list, removing padding as necessary
   * @param existPreferences
   * @param instanceName
   * @param numReplicas
   * @return A new preference list with the instance added to the end (if it did not already exist)
   */
  private static List<String> addInstanceToPreferenceList(List<String> existPreferences,
      String instanceName, int numReplicas) {
    if (existPreferences == null) {
      existPreferences = Lists.newArrayList();
    }
    List<String> newPreferences = Lists.newArrayListWithCapacity(numReplicas);
    for (String existInstance : existPreferences) {
      if (!existInstance.isEmpty()) {
        newPreferences.add(existInstance);
      }
    }
    if (!newPreferences.contains(instanceName)) {
      newPreferences.add(instanceName);
    }
    addDummiesToPreferenceList(newPreferences, numReplicas);
    return newPreferences;
  }

  /**
   * Remove an instance from a preference list, padding as necessary
   * @param existPreferences
   * @param instanceName
   * @param numReplicas
   * @return new preference list with the instance removed (if it existed)
   */
  private static List<String> removeInstanceFromPreferenceList(List<String> existPreferences,
      String instanceName, int numReplicas) {
    if (existPreferences == null) {
      existPreferences = Lists.newArrayList();
    }
    List<String> newPreferences = Lists.newArrayListWithCapacity(numReplicas);
    for (String existInstance : existPreferences) {
      if (!existInstance.isEmpty() && !existInstance.equals(instanceName)) {
        newPreferences.add(existInstance);
      }
    }
    addDummiesToPreferenceList(newPreferences, numReplicas);
    return newPreferences;
  }

  /**
   * Pad a preference list with empty strings to have it reach numReplicas size
   * @param preferences
   * @param numReplicas
   */
  private static void addDummiesToPreferenceList(List<String> preferences, int numReplicas) {
    int numRemaining = numReplicas - preferences.size();
    numRemaining = (numRemaining > 0) ? numRemaining : 0;
    for (int i = 0; i < numRemaining; i++) {
      preferences.add("");
    }
  }

  /**
   * A state model whose callbacks correspond to when to run a task. When becoming online, the task
   * should be run, and then the instance should be removed from the preference list for the task
   * (padded by spaces). All other transitions are no-ops.
   */
  public class PrefListTaskOnlineOfflineStateModel extends TransitionHandler {
    /**
     * Run the task. The parallelism of this is dictated by the constraints that are set.
     * @param message
     * @param context
     * @throws InterruptedException
     */
    public void onBecomeOnlineFromOffline(final Message message, NotificationContext context)
        throws InterruptedException {
      // Do the work, and then finally remove the instance from the preference list for this
      // partition
      HelixManager manager = context.getManager();
      LOG.info("START onBecomeOnlineFromOffline for " + message.getPartitionName() + " on "
          + manager.getInstanceName());
      int oldSize;
      synchronized (_instanceList) {
        oldSize = _instanceList.size();
        _instanceList.add(manager.getInstanceName());
      }
      Assert.assertEquals(oldSize, 0); // ensure these transitions are fully synchronized

      Thread.sleep(TRANSITION_TIME); // a sleep simulates work

      // Need to disable in order to get the transition the next time
      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      removeInstanceFromPreferences(accessor, manager.getInstanceName(), message.getResourceName(),
          message.getPartitionName());
      LOG.info("FINISH onBecomeOnlineFromOffline for " + message.getPartitionName() + " on "
          + manager.getInstanceName());

      int newSize;
      synchronized (_instanceList) {
        _instanceList.remove(_instanceList.size() - 1);
        newSize = _instanceList.size();
      }
      Assert.assertEquals(newSize, oldSize); // ensure nothing came in during this time
      _onlineLatch.countDown();
    }

    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      HelixManager manager = context.getManager();
      LOG.info("onBecomeOfflineFromOnline for " + message.getPartitionName() + " on "
          + manager.getInstanceName());
    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      HelixManager manager = context.getManager();
      LOG.info("onBecomeDroppedFromOffline for " + message.getPartitionName() + " on "
          + manager.getInstanceName());
      _offlineLatch.countDown();
    }

    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      HelixManager manager = context.getManager();
      LOG.info("onBecomeOfflineFromError for " + message.getPartitionName() + " on "
          + manager.getInstanceName());
    }
  }

  public class PrefListTaskOnlineOfflineStateModelFactory extends
      StateTransitionHandlerFactory<PrefListTaskOnlineOfflineStateModel> {
    @Override
    public PrefListTaskOnlineOfflineStateModel createStateTransitionHandler(ResourceId resource,
        PartitionId partition) {
      return new PrefListTaskOnlineOfflineStateModel();
    }
  }
}
