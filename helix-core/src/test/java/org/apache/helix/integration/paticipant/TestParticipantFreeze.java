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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.ClusterManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.util.MessageUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestParticipantFreeze extends ZkTestBase {
  private HelixManager _manager;
  private HelixDataAccessor _accessor;
  private PropertyKey.Builder _keyBuilder;
  private String _clusterName;
  private int _numNodes;
  private String _resourceName;
  private String _instanceName;
  private MockParticipantManager[] _participants;
  // current states in participant[0]
  private List<CurrentState> _originCurStates;
  private String _originSession;

  @BeforeClass
  public void beforeClass() throws Exception {
    _clusterName = "CLUSTER_" + TestHelper.getTestClassName();
    _numNodes = 3;
    _resourceName = "TestDB";
    _participants = new MockParticipantManager[_numNodes];
    TestHelper.setupCluster(_clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        _resourceName, // resource name prefix
        1, // resources
        1, // partitions per resource
        _numNodes, // number of nodes
        3, // replicas
        "MasterSlave", true);

    _manager = HelixManagerFactory
        .getZKHelixManager(_clusterName, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
    _accessor = _manager.getHelixDataAccessor();
    _keyBuilder = _accessor.keyBuilder();

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, _clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < _numNodes; i++) {
      String instanceName = "localhost_" + (12918 + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, _clusterName, instanceName);
      _participants[i].syncStart();
    }
    _instanceName = _participants[0].getInstanceName();

    Assert.assertTrue(ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, _clusterName)));

    // We just need controller to rebalance the cluster once to get current states.
    controller.syncStop();

    _originSession = _participants[0].getSessionId();
    _originCurStates =
        _accessor.getChildValues(_keyBuilder.currentStates(_instanceName, _originSession), false);
  }

  @AfterClass
  public void afterClass() {
    _manager.disconnect();
    Arrays.stream(_participants).forEach(ClusterManager::syncStop);
    deleteCluster(_clusterName);
  }

  /*
   * Live instance is not frozen and does not have a frozen status field
   */
  @Test
  public void testNormalLiveInstanceStatus() {
    LiveInstance liveInstance = _accessor.getProperty(_keyBuilder.liveInstance(_instanceName));
    Assert.assertEquals(liveInstance.getStatus(), LiveInstance.LiveInstanceStatus.NORMAL);
    Assert.assertNull(
        liveInstance.getRecord().getSimpleField(LiveInstance.LiveInstanceProperty.STATUS.name()));
  }

  @Test(dependsOnMethods = "testNormalLiveInstanceStatus")
  public void testFreezeParticipant() throws Exception {
    freezeParticipant(_participants[0]);
  }

  // Simulates instance is restarted and the in-memory status is gone.
  // When instance comes back alive, it'll reset state model, carry over
  // and set current state to init state.
  @Test(dependsOnMethods = "testFreezeParticipant")
  public void testRestartParticipantWhenFrozen() throws Exception {
    String instanceName = _participants[1].getInstanceName();
    List<CurrentState> originCurStates = _accessor
        .getChildValues(_keyBuilder.currentStates(instanceName, _participants[1].getSessionId()),
            false);
    String oldSession = _participants[1].getSessionId();
    freezeParticipant(_participants[1]);

    // Restart participants[1]
    _participants[1].syncStop();
    _participants[1] = new MockParticipantManager(ZK_ADDR, _participants[1].getClusterName(),
        instanceName);
    _participants[1].syncStart();

    Assert.assertTrue(TestHelper.verify(() ->
            _gZkClient.exists(_keyBuilder.liveInstance(instanceName).getPath()),
        TestHelper.WAIT_DURATION));
    LiveInstance liveInstance = _accessor.getProperty(_keyBuilder.liveInstance(instanceName));

    // New live instance ephemeral node
    Assert.assertEquals(liveInstance.getEphemeralOwner(), _participants[1].getSessionId());
    // Status is not frozen because controller is not running, no freeze message sent.
    verifyLiveInstanceStatus(_participants[1], LiveInstance.LiveInstanceStatus.NORMAL);

    // Old session current state is deleted because of current state carry-over
    Assert.assertTrue(TestHelper.verify(
        () -> !_gZkClient.exists(_keyBuilder.currentStates(instanceName, oldSession).getPath()),
        TestHelper.WAIT_DURATION));

    // Current states are set to init states (OFFLINE)
    List<CurrentState> curStates = _accessor
        .getChildValues(_keyBuilder.currentStates(instanceName, _participants[1].getSessionId()),
            false);
    Assert.assertEquals(curStates.size(), 1);
    Assert.assertTrue(TestHelper.verify(() -> {
      for (CurrentState cs : originCurStates) {
        String stateModelDefRef = cs.getStateModelDefRef();
        for (String partition : cs.getPartitionStateMap().keySet()) {
          StateModelDefinition stateModelDef =
              _accessor.getProperty(_keyBuilder.stateModelDef(stateModelDefRef));
          String initState = stateModelDef.getInitialState();
          if (!initState.equals(curStates.get(0).getPartitionStateMap().get(partition))) {
            return false;
          }
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION));
  }

  // Simulates session expires but in-memory status is still kept.
  // No state model reset or current state carry-over
  @Test(dependsOnMethods = "testRestartParticipantWhenFrozen")
  public void testHandleNewSessionWhenFrozen() throws Exception {
    // there are current states for the resource
    Assert.assertFalse(_originCurStates.isEmpty());

    ZkTestHelper.expireSession(_participants[0].getZkClient());
    String currentSession = _participants[0].getSessionId();
    Assert.assertFalse(_originSession.equals(currentSession));

    Assert.assertTrue(TestHelper.verify(() ->
            _gZkClient.exists(_keyBuilder.liveInstance(_instanceName).getPath()),
            TestHelper.WAIT_DURATION));
    LiveInstance liveInstance = _accessor.getProperty(_keyBuilder.liveInstance(_instanceName));

    // New live instance ephemeral node with FROZEN status
    Assert.assertFalse(_originSession.equals(liveInstance.getEphemeralOwner()));
    Assert.assertEquals(liveInstance.getStatus(), LiveInstance.LiveInstanceStatus.FROZEN);

    // New session path does not exist since no current state carry over for the current session.
    Assert.assertFalse(
        _gZkClient.exists(_keyBuilder.currentStates(_instanceName, currentSession).getPath()));
    // Old session CS still exist.
    Assert.assertTrue(
        _gZkClient.exists(_keyBuilder.currentStates(_instanceName, _originSession).getPath()));
  }

  @Test(dependsOnMethods = "testHandleNewSessionWhenFrozen")
  public void testUnfreezeParticipant() throws Exception {
    Message unfreezeMessage = MessageUtil
        .createStatusChangeMessage(LiveInstance.LiveInstanceStatus.FROZEN,
            LiveInstance.LiveInstanceStatus.NORMAL, _manager.getInstanceName(),
            _manager.getSessionId(), _instanceName, _participants[0].getSessionId());
    List<PropertyKey> keys = Collections
        .singletonList(_keyBuilder.message(unfreezeMessage.getTgtName(), unfreezeMessage.getId()));

    boolean[] success = _accessor.createChildren(keys, Collections.singletonList(unfreezeMessage));
    Assert.assertTrue(success[0]);

    // Live instance status is NORMAL, but set to null value in both memory and zk.
    // After live instance status is updated, the process is completed.
    verifyLiveInstanceStatus(_participants[0], LiveInstance.LiveInstanceStatus.NORMAL);
    // Unfreeze message is correctly deleted
    Assert.assertNull(
        _accessor.getProperty(_keyBuilder.message(_instanceName, unfreezeMessage.getId())));

    // current state is carried over
    List<CurrentState> curStates = _accessor
        .getChildValues(_keyBuilder.currentStates(_instanceName, _participants[0].getSessionId()),
            false);
    Assert.assertFalse(curStates.isEmpty());
    // The original current states are deleted.
    Assert.assertFalse(
        _gZkClient.exists(_keyBuilder.currentStates(_instanceName, _originSession).getPath()));

    // current states should be the same as the original current states
    // with CS carry-over when unfreezing
    Assert.assertTrue(verifyCurrentStates(_originCurStates, curStates));
  }

  private void verifyLiveInstanceStatus(MockParticipantManager participant,
      LiveInstance.LiveInstanceStatus status) throws Exception {
    // Verify live instance status in both memory and zk
    Assert.assertTrue(TestHelper.verify(() -> {
      LiveInstance.LiveInstanceStatus inMemoryLiveInstanceStatus =
          ((DefaultMessagingService) participant.getMessagingService()).getExecutor()
              .getLiveInstanceStatus();
      return inMemoryLiveInstanceStatus == status;
    }, TestHelper.WAIT_DURATION));

    Assert.assertTrue(TestHelper.verify(() -> {
      LiveInstance liveInstance =
          _accessor.getProperty(_keyBuilder.liveInstance(participant.getInstanceName()));
      return liveInstance.getStatus() == status;
    }, TestHelper.WAIT_DURATION));
  }

  private boolean verifyCurrentStates(List<CurrentState> originCurStates,
      List<CurrentState> curStates) {
    for (CurrentState ocs : originCurStates) {
      for (CurrentState cs : curStates) {
        if (cs.getId().equals(ocs.getId())
            && !cs.getPartitionStateMap().equals(ocs.getPartitionStateMap())) {
          return false;
        }
      }
    }
    return true;
  }

  private void freezeParticipant(MockParticipantManager participant) throws Exception {
    Message freezeMessage = MessageUtil
        .createStatusChangeMessage(LiveInstance.LiveInstanceStatus.NORMAL,
            LiveInstance.LiveInstanceStatus.FROZEN, _manager.getInstanceName(),
            _manager.getSessionId(), participant.getInstanceName(), participant.getSessionId());

    List<PropertyKey> keys = Collections
        .singletonList(_keyBuilder.message(freezeMessage.getTgtName(), freezeMessage.getId()));

    boolean[] success = _accessor.createChildren(keys, Collections.singletonList(freezeMessage));
    Assert.assertTrue(success[0]);

    // Live instance status is frozen in both memory and zk
    verifyLiveInstanceStatus(participant, LiveInstance.LiveInstanceStatus.FROZEN);
    // Freeze message is correctly deleted
    Assert.assertTrue(TestHelper.verify(() -> !_gZkClient.exists(
        _keyBuilder.message(participant.getInstanceName(), freezeMessage.getId()).getPath()),
        TestHelper.WAIT_DURATION));
  }
}
