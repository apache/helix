package org.apache.helix.integration.paticipant;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.messaging.handling.MessageHandler.ErrorCode;
import org.apache.helix.mock.participant.MockMSStateModel;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.mock.participant.SleepTransition;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestStateTransitionTimeout extends ZkStandAloneCMTestBase {
  // See beforeClass for why the transition timeout must not be too short.
  private static final int TRANSITION_TIMEOUT_MS = 3000;
  private static final int HANDLER_SLEEP_MS = 60000;
  private static Logger LOG = LoggerFactory.getLogger(TestStateTransitionTimeout.class);

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, _PARTITIONS, STATE_MODEL);

    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 3);

    // Set the timeout value. It must be comfortably longer than the time it takes the participant
    // to merely start executing a state transition task: HelixTaskExecutor arms the timeout timer
    // right after submitting the task to its thread pool, and if the timer fires before the task
    // has started, the task is cancelled without ever running, so no ERROR state is produced and
    // the message is never cleaned up. With a 200ms timeout that happened regularly on slow CI
    // machines (thread creation, GC pauses, ZK latency). The handler sleep in the test is much
    // longer than this timeout, so the timeout still always fires before the transition finishes.
    String stateTransition = "SLAVE" + "-" + "MASTER" + "_" + Message.Attributes.TIMEOUT;
    String command =
        "-zkSvr " + ZK_ADDR + " -addResourceProperty " + CLUSTER_NAME + " " + TEST_DB + " "
            + stateTransition + " " + TRANSITION_TIMEOUT_MS;
    ClusterSetup.processCommandLineArgs(command.split(" "));
  }

  @StateModelInfo(initialState = "OFFLINE", states = {
      "MASTER", "SLAVE", "ERROR"
  })
  public static class TimeOutStateModel extends MockMSStateModel {
    // Only the SLAVE->MASTER transition of the selected partitions sleeps. The transition is kept
    // out of MockMSStateModel's generic handler on purpose: passing it to the super constructor
    // would make every transition (including the OFFLINE->SLAVE bootstrap of all partitions on
    // all nodes) sleep for the full duration, which is not what the test exercises and made the
    // test take a full sleep per stage.
    private final MockTransition _sleepTransition;
    boolean _sleep = false;
    // Written by the message handling thread and read by the test thread.
    volatile StateTransitionError _error;
    volatile int _errorCallcount = 0;

    public TimeOutStateModel(MockTransition transition, boolean sleep) {
      super(null);
      _sleepTransition = transition;
      _sleep = sleep;
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context)
        throws InterruptedException {
      LOG.info("Become MASTER from SLAVE");
      if (_sleepTransition != null && _sleep) {
        _sleepTransition.doTransition(message, context);
      }
    }

    @Override
    public void rollbackOnError(Message message, NotificationContext context,
        StateTransitionError error) {
      _error = error;
      _errorCallcount++;
    }
  }

  public static class SleepStateModelFactory extends StateModelFactory<TimeOutStateModel> {
    Set<String> partitionsToSleep = new HashSet<String>();
    int _sleepTime;

    public SleepStateModelFactory(int sleepTime) {
      _sleepTime = sleepTime;
    }

    public void setPartitions(Collection<String> partitions) {
      partitionsToSleep.addAll(partitions);
    }

    public void addPartition(String partition) {
      partitionsToSleep.add(partition);
    }

    @Override
    public TimeOutStateModel createNewStateModel(String resource, String stateUnitKey) {
      return new TimeOutStateModel(new SleepTransition(_sleepTime),
          partitionsToSleep.contains(stateUnitKey));
    }
  }

  @Test
  public void testStateTransitionTimeOut() throws Exception {
    Map<String, SleepStateModelFactory> factories = new HashMap<String, SleepStateModelFactory>();
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      // Sleep much longer than the transition timeout so the timeout always fires before the
      // transition can finish, even if the timeout timer is delayed on a slow machine. The sleep
      // is interrupted once the timeout fires, so a larger value does not slow down the timed-out
      // transitions.
      SleepStateModelFactory factory = new SleepStateModelFactory(HANDLER_SLEEP_MS);
      factories.put(instanceName, factory);
      for (String p : idealState.getPartitionSet()) {
        if (idealState.getPreferenceList(p).get(0).equals(instanceName)) {
          factory.addPartition(p);
        }
      }

      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].getStateMachineEngine().registerStateModelFactory("MasterSlave", factory);
      _participants[i].syncStart();
    }
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller =
        new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    boolean result =
        ClusterStateVerifier
            .verifyByPolling(new MasterNbInExtViewVerifier(ZK_ADDR, CLUSTER_NAME),
                2 * TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);
    HelixDataAccessor accessor = _participants[0].getHelixDataAccessor();

    // Keep polling for the expected end state and assert on the polling result, so the test
    // tolerates slow controller pipelines / message handling on overloaded machines.
    Assert.assertTrue(TestHelper.verify(() -> verify(accessor, idealState, factories),
        2 * TestHelper.WAIT_DURATION));
  }

  private boolean verify(HelixDataAccessor accessor, IdealState idealState,
      Map<String, SleepStateModelFactory> factoryMap) {
    Builder kb = accessor.keyBuilder();
    ExternalView ev = accessor.getProperty(kb.externalView(TEST_DB));
    if (ev == null) {
      return false;
    }
    for (String p : idealState.getPartitionSet()) {
      String idealMaster = idealState.getPreferenceList(p).get(0);
      // The external view may not contain the partition / instance state yet; return false
      // instead of throwing so that the caller can keep polling.
      Map<String, String> stateMap = ev.getStateMap(p);
      if (stateMap == null || !"ERROR".equals(stateMap.get(idealMaster))) {
        return false;
      }

      TimeOutStateModel model = factoryMap.get(idealMaster).getStateModel(TEST_DB, p);
      if (model == null || model._errorCallcount != 1
          || model._error.getCode() != ErrorCode.TIMEOUT) {
        return false;
      }
    }

    return true;
  }
}
