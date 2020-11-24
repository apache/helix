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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPerReplicaThrottle extends ZkTestBase {
  private static Logger LOG = LoggerFactory.getLogger(LatchedTransition.class);

  private String _resourceName = "TestDB";
  private String _clusterName = getShortClassName();
  private HelixDataAccessor _accessor;
  private MockParticipantManager[] _participants;

  /**
   * Set up delayed rebalancer and minimum active replica settings to mimic user's use case.
   * @param clusterName
   * @param participantCount
   * @param stateModelDef
   * @throws Exception
   */
  private void setupCluster(String clusterName, int participantCount, String stateModelDef ) throws Exception {
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant start port
        "localhost", // participant name prefix
        _resourceName, // resource name prefix
        1, // resources
        3, // partitions per resource
        participantCount, // number of nodes
        3, // replicas
        stateModelDef, IdealState.RebalanceMode.FULL_AUTO, true); // do rebalance

    // Enable DelayedAutoRebalance
    ClusterConfig clusterConfig = _accessor.getProperty(_accessor.keyBuilder().clusterConfig());
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(1800000L);
    _accessor.setProperty(_accessor.keyBuilder().clusterConfig(), clusterConfig);

    // Set minActiveReplicas at 2
    List<String> idealStates = _accessor.getChildNames(_accessor.keyBuilder().idealStates());
    for (String is : idealStates) {
      IdealState idealState = _accessor.getProperty(_accessor.keyBuilder().idealStates(is));
      idealState.setMinActiveReplicas(2);
      idealState.setRebalanceStrategy(
          "org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy");
      idealState
          .setRebalancerClassName("org.apache.helix.controller.rebalancer.DelayedAutoRebalancer");
      _accessor.setProperty(_accessor.keyBuilder().idealStates(is), idealState);
    }
  }

  static class LatchedTransition extends MockTransition {
    protected static int batchSize = 10;
    protected static ArrayList<CountDownLatch> latches;
    static {
      latches = new ArrayList<>();
      for (int i = 0; i < batchSize; i++) {
        latches.add(new CountDownLatch(1));
      }
    }
    protected static AtomicInteger latchStep = new AtomicInteger(0);
    protected static AtomicInteger waitCnt = new AtomicInteger(0);

    protected String _id;
    LatchedTransition(String id) {
      _id = id;
    }

    @Override
    public void doTransition(Message message, NotificationContext context)
        throws InterruptedException {
      waitCnt.addAndGet(1);
      if (latchStep.get() > latches.size()) {
        Assert.fail("latchStep out of bound");
        return;
      }
      LOG.info("id {} before wait for latch, step {} waitCnt{}, msg: {}",
          _id, latchStep.get(), waitCnt.get(), message);
      latches.get(latchStep.get()).await();
      LOG.info("id {} after wait for latch, step {} waitCnt{}, msg: {}",
          _id, latchStep.get(), waitCnt.get(), message);
    }

    public static int getWaitCnt() {
      return waitCnt.get();
    }

    public static void incStepAndClearWaitCnt() {
      waitCnt.set(0);
      int step = latchStep.getAndIncrement();
      latches.get(step).countDown();
      if (step >= latches.size()) {
        for (int i = 0; i < batchSize; i++) {
          latches.add(new CountDownLatch(1));
        }
      }
    }

    public static void reset() {
      latches = new ArrayList<>();
      for (int i = 0; i < batchSize; i++) {
        latches.add(new CountDownLatch(1));
      }
      latchStep.set(0);
      waitCnt.set(0);
    }
  }

  /**
   * Set up the cluster and pause the controller.
   * @param participantCount
   * @throws Exception
   */
  private void setupEnvironment(int participantCount, String stateModelDef) throws Exception {
    _participants = new MockParticipantManager[participantCount];

    _accessor = new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    setupCluster(_clusterName, participantCount, stateModelDef);

    LatchedTransition.reset();

    // Start _participants
    for (int i = 0; i < participantCount; i++) {
      _participants[i] =
          new MockParticipantManager(ZK_ADDR, _clusterName, "localhost_" + (12918 + i));
      _participants[i].setMockOFFactory();
      LatchedTransition transition = new LatchedTransition(_participants[i].getInstanceName());
      _participants[i].setTransition(transition);
      _participants[i].syncStart();
    }
  }

  private void setThrottleConfig() {
    PropertyKey.Builder keyBuilder = _accessor.keyBuilder();

    ClusterConfig clusterConfig = _accessor.getProperty(_accessor.keyBuilder().clusterConfig());
    clusterConfig.setResourcePriorityField("Name");
    List<StateTransitionThrottleConfig> throttleConfigs = new ArrayList<>();

    // Add throttling at cluster-level
    throttleConfigs.add(new StateTransitionThrottleConfig(
        StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
        StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 6));
    throttleConfigs.add(
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 6));
    throttleConfigs
        .add(new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.ANY,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 6));

    // Add throttling at instance level
    throttleConfigs
        .add(new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.ANY,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, 3));

    clusterConfig.setStateTransitionThrottleConfigs(throttleConfigs);
    _accessor.setProperty(keyBuilder.clusterConfig(), clusterConfig);
  }

  private void setThrottleConfig(Map<StateTransitionThrottleConfig.ThrottleScope,
      Map<StateTransitionThrottleConfig.RebalanceType, Long>> configs) {
    PropertyKey.Builder keyBuilder = _accessor.keyBuilder();

    ClusterConfig clusterConfig = _accessor.getProperty(_accessor.keyBuilder().clusterConfig());
    clusterConfig.setResourcePriorityField("Name");

    List<StateTransitionThrottleConfig> throttleConfigs = clusterConfig.getStateTransitionThrottleConfigs();
    Map<StateTransitionThrottleConfig.ThrottleScope,
        Map<StateTransitionThrottleConfig.RebalanceType, Long>> existConfigs = new HashMap<>();
    for (StateTransitionThrottleConfig config : throttleConfigs) {
      StateTransitionThrottleConfig.ThrottleScope scope = config.getThrottleScope();
      StateTransitionThrottleConfig.RebalanceType type = config.getRebalanceType();
      Long threshold = config.getMaxPartitionInTransition();
      Map<StateTransitionThrottleConfig.RebalanceType, Long> typeLongMap =
          existConfigs.getOrDefault(scope, new HashMap<>());
      typeLongMap.put(type, threshold);
      existConfigs.put(scope, typeLongMap);
    }

    for (StateTransitionThrottleConfig.ThrottleScope scopeKey : configs.keySet()) {
      for (StateTransitionThrottleConfig.RebalanceType rebalanceKey: configs.get(scopeKey).keySet()) {
        Long  threshold = configs.get(scopeKey).get(rebalanceKey);
        Map<StateTransitionThrottleConfig.RebalanceType, Long> typeLongMap =
            existConfigs.getOrDefault(scopeKey, new HashMap<>());
        typeLongMap.put(rebalanceKey, threshold);
        existConfigs.put(scopeKey, typeLongMap);
      }
    }

    List<StateTransitionThrottleConfig> throttleNewConfigs = new ArrayList<>();
    for (StateTransitionThrottleConfig.ThrottleScope scopeKey : existConfigs.keySet()) {
      for (StateTransitionThrottleConfig.RebalanceType rebalanceKey : existConfigs.get(scopeKey).keySet()) {
        Long threshold = existConfigs.get(scopeKey).get(rebalanceKey);
        StateTransitionThrottleConfig throttleConfig =
            new StateTransitionThrottleConfig(rebalanceKey, scopeKey, threshold);
        throttleNewConfigs.add(throttleConfig);
      }
    }

    clusterConfig.setStateTransitionThrottleConfigs(throttleNewConfigs);
    _accessor.setProperty(keyBuilder.clusterConfig(), clusterConfig);
  }

  @Test
  public void testOnlineOfflineStepByStep() throws Exception {
    int participantCount = 3;
    setupEnvironment(participantCount, "OnlineOffline");
    setThrottleConfig();

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, _clusterName, "controller_0");
    controller.syncStart();

    Assert.assertTrue(TestHelper.verify(()-> {
      return LatchedTransition.getWaitCnt() == participantCount * 2;
    }, TestHelper.WAIT_DURATION));

    Map<String, Integer> partitionMsgCount = new HashMap<>();
    for (MockParticipantManager participantManager : _participants) {
      PropertyKey key = _accessor.keyBuilder().messages(participantManager.getInstanceName());
      List<Message> messages = _accessor.getChildValues(key, true);
      for (Message msg : messages) {
        String partName = msg.getPartitionName();
        String toState = msg.getToState();
        Assert.assertTrue("ONLINE".equals(toState));
        if (!partitionMsgCount.containsKey(partName)) {
          partitionMsgCount.put(partName, 0);
        }
        partitionMsgCount.put(partName, partitionMsgCount.get(partName) + 1);
      }
    }

    for (String partition: partitionMsgCount.keySet()) {
      Assert.assertTrue(partitionMsgCount.get(partition).equals(2));
    }

    // Second step
    LatchedTransition.incStepAndClearWaitCnt();

    Assert.assertTrue(TestHelper.verify(()-> {
      return LatchedTransition.getWaitCnt() == participantCount;
    }, TestHelper.WAIT_DURATION));

    partitionMsgCount = new HashMap<>();
    for (MockParticipantManager participantManager : _participants) {
      PropertyKey key = _accessor.keyBuilder().messages(participantManager.getInstanceName());
      List<Message> messages = _accessor.getChildValues(key, true);
      for (Message msg : messages) {
        String partName = msg.getPartitionName();
        String toState = msg.getToState();
        Assert.assertTrue("ONLINE".equals(toState));
        if (!partitionMsgCount.containsKey(partName)) {
          partitionMsgCount.put(partName, 0);
        }
        partitionMsgCount.put(partName, partitionMsgCount.get(partName) + 1);
      }
    }

    for (String partition: partitionMsgCount.keySet()) {
      Assert.assertTrue(partitionMsgCount.get(partition).equals(1));
    }
  }

  @Test
  public void testMasterSlaveStepByStep() throws Exception {
    int participantCount = 3;
    setupEnvironment(participantCount, "MasterSlave");
    setThrottleConfig();

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, _clusterName, "controller_0");
    controller.syncStart();

    // first step 6 recovery messages for 3 partitions would be sent out. They are all O->S.
    // each partition would have 2 messages as offline to slave. Note, the 3rd O-> are deemed
    // as load and would be throttled.
    Assert.assertTrue(TestHelper.verify(()-> {
      return LatchedTransition.getWaitCnt() == participantCount * 2;
    }, TestHelper.WAIT_DURATION));

    Map<String, Integer> partitionMsgCount = new HashMap<>();
    for (MockParticipantManager participantManager : _participants) {
      PropertyKey key = _accessor.keyBuilder().messages(participantManager.getInstanceName());
      List<Message> messages = _accessor.getChildValues(key, true);
      for (Message msg : messages) {
        String partName = msg.getPartitionName();
        String toState = msg.getToState();
        Assert.assertTrue("SLAVE".equals(toState));
        if (!partitionMsgCount.containsKey(partName)) {
          partitionMsgCount.put(partName, 0);
        }
        partitionMsgCount.put(partName, partitionMsgCount.get(partName) + 1);
      }
    }

    for (String partition: partitionMsgCount.keySet()) {
        Assert.assertTrue(partitionMsgCount.get(partition).equals(2));
    }

    // Second step, change cluster level throttle to 3 for ANY type. Here, recovery would have
    // high priority. Thus, three S->M for each partition would be sent out. Note, the O->S for the
    // 3rd replica for each partition is still throttle as load rebalance.
    Map<StateTransitionThrottleConfig.ThrottleScope,
        Map<StateTransitionThrottleConfig.RebalanceType, Long>> configs = new HashMap<>();
    Map<StateTransitionThrottleConfig.RebalanceType, Long> typeLongMap = new HashMap<>();
    typeLongMap.put(StateTransitionThrottleConfig.RebalanceType.ANY, new Long(3));
    configs.put(StateTransitionThrottleConfig.ThrottleScope.CLUSTER, typeLongMap);
    setThrottleConfig(configs);

    LatchedTransition.incStepAndClearWaitCnt();

    Assert.assertTrue(TestHelper.verify(()-> {
      return LatchedTransition.getWaitCnt() == participantCount;
    }, TestHelper.WAIT_DURATION));

    partitionMsgCount = new HashMap<>();
    for (MockParticipantManager participantManager : _participants) {
      PropertyKey key = _accessor.keyBuilder().messages(participantManager.getInstanceName());
      List<Message> messages = _accessor.getChildValues(key, true);
      for (Message msg : messages) {
        String partName = msg.getPartitionName();
        String toState = msg.getToState();
        Assert.assertTrue("MASTER".equals(toState));
        if (!partitionMsgCount.containsKey(partName)) {
          partitionMsgCount.put(partName, 0);
        }
        partitionMsgCount.put(partName, partitionMsgCount.get(partName) + 1);
      }
    }

    for (String partition: partitionMsgCount.keySet()) {
      Assert.assertTrue(partitionMsgCount.get(partition).equals(1));
    }

    // Finally, third step. The rest three O->S load messages for each partition would be sent out.
    LatchedTransition.incStepAndClearWaitCnt();

    Assert.assertTrue(TestHelper.verify(()-> {
      return LatchedTransition.getWaitCnt() == participantCount;
    }, TestHelper.WAIT_DURATION));

    partitionMsgCount = new HashMap<>();
    for (MockParticipantManager participantManager : _participants) {
      PropertyKey key = _accessor.keyBuilder().messages(participantManager.getInstanceName());
      List<Message> messages = _accessor.getChildValues(key, true);
      for (Message msg : messages) {
        String partName = msg.getPartitionName();
        String toState = msg.getToState();
        Assert.assertTrue("SLAVE".equals(toState));
        if (!partitionMsgCount.containsKey(partName)) {
          partitionMsgCount.put(partName, 0);
        }
        partitionMsgCount.put(partName, partitionMsgCount.get(partName) + 1);
      }
    }

    for (String partition: partitionMsgCount.keySet()) {
      Assert.assertTrue(partitionMsgCount.get(partition).equals(1));
    }

    // clean up the cluster
    controller.syncStop();
    for (int i = 0; i < participantCount; i++) {
      _participants[i].syncStop();
    }
    deleteCluster(_clusterName);
  }
}
