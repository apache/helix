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
 *   http://www.apacahe.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestNoThrottleDisabledPartitions extends ZkTestBase {
  private String _resourceName = "TestDB";
  private String _clusterName = getShortClassName();
  private HelixDataAccessor _accessor;
  private MockParticipantManager[] _participants;

  /**
   * Given the following setup for a partition:
   * instance 1 : M
   * instance 2 : S
   * instance 3 : S
   * and no throttle config set for recovery balance
   * and throttle config of 1 set for load balance,
   * test that disabling instance 1 puts this partition in recovery balance, so that all transitions
   * for a partition go through.
   * * instance 1 : S (M->S->Offline)
   * * instance 2 : M (S->M because it's in recovery)
   * * instance 3 : S
   * @throws Exception
   */
  @Test
  public void testDisablingTopStateReplicaByDisablingInstance() throws Exception {
    int participantCount = 5;
    setupEnvironment(participantCount);

    // Set the throttling only for load balance
    setThrottleConfigForLoadBalance();

    // Disable instance 0 so that it will cause a partition to do a recovery balance
    PropertyKey key = _accessor.keyBuilder().instanceConfig(_participants[0].getInstanceName());
    InstanceConfig instanceConfig = _accessor.getProperty(key);
    instanceConfig.setInstanceEnabled(false);
    _accessor.setProperty(key, instanceConfig);

    // Resume the controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, _clusterName, "controller_0");
    controller.syncStart();
    Thread.sleep(500L);

    // The disabled instance should not hold any top state replicas (MASTER)
    PropertyKey liveInstanceKey =
        _accessor.keyBuilder().liveInstance(_participants[0].getInstanceName());
    LiveInstance liveInstance = _accessor.getProperty(liveInstanceKey);
    if (liveInstance != null) {
      String sessionId = liveInstance.getSessionId();
      List<CurrentState> currentStates = _accessor.getChildValues(
          _accessor.keyBuilder().currentStates(_participants[0].getInstanceName(), sessionId));
      for (CurrentState currentState : currentStates) {
        for (Map.Entry<String, String> partitionState : currentState.getPartitionStateMap()
            .entrySet()) {
          Assert.assertFalse(partitionState.getValue().equals("MASTER"));
        }
      }
    }

    // clean up the cluster
    controller.syncStop();
    for (int i = 0; i < participantCount; i++) {
      _participants[i].syncStop();
    }
    deleteCluster(_clusterName);
  }

  /**
   * Given the following setup for a partition:
   * instance 1 : M
   * instance 2 : S
   * instance 3 : S
   * and no throttle config set for recovery balance
   * and throttle config of 1 set for load balance,
   * Instead of disabling the instance, we disable the partition in the instance config.
   * * instance 1 : S (M->S->Offline)
   * * instance 2 : M (S->M because it's in recovery)
   * * instance 3 : S
   * @throws Exception
   */
  @Test
  public void testDisablingPartitionOnInstance() throws Exception {
    int participantCount = 5;
    setupEnvironment(participantCount);

    // Set the throttling only for load balance
    setThrottleConfigForLoadBalance();

    // In this setup, TestDB0_2 has a MASTER replica on localhost_12918
    disablePartitionOnInstance(_participants[0], _resourceName + "0", "TestDB0_2");

    // Resume the controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, _clusterName, "controller_0");
    controller.syncStart();
    Thread.sleep(500L);

    // The disabled instance should not hold any top state replicas (MASTER)
    PropertyKey liveInstanceKey =
        _accessor.keyBuilder().liveInstance(_participants[0].getInstanceName());
    LiveInstance liveInstance = _accessor.getProperty(liveInstanceKey);
    if (liveInstance != null) {
      String sessionId = liveInstance.getSessionId();
      List<CurrentState> currentStates = _accessor.getChildValues(
          _accessor.keyBuilder().currentStates(_participants[0].getInstanceName(), sessionId));
      for (CurrentState currentState : currentStates) {
        for (Map.Entry<String, String> partitionState : currentState.getPartitionStateMap()
            .entrySet()) {
          if (partitionState.getKey().equals("TestDB0_2")) {
            Assert.assertFalse(partitionState.getValue().equals("MASTER"));
          }
        }
      }
    }

    // clean up the cluster
    controller.syncStop();
    for (int i = 0; i < participantCount; i++) {
      _participants[i].syncStop();
    }
    deleteCluster(_clusterName);
  }

  /**
   * Given the following setup for a partition:
   * instance 1 : M
   * instance 2 : S
   * instance 3 : S
   * and no throttle config set for recovery balance
   * and throttle config of 1 set for load balance,
   * Instead of disabling the instance, we disable the partition in the instance config.
   * Here, we set the recovery balance config. Then we should still see the MASTER.
   * * instance 1 : S (M->S->Offline)
   * * instance 2 : M (S->M because it's in recovery)
   * * instance 3 : S
   * @throws Exception
   */
  @Test
  public void testDisablingPartitionOnInstanceWithRecoveryThrottle() throws Exception {
    int participantCount = 5;
    setupEnvironment(participantCount);

    // Set the throttling
    setThrottleConfigForLoadBalance();
    setThrottleConfigForRecoveryBalance();

    // In this setup, TestDB0_2 has a MASTER replica on localhost_12918
    disablePartitionOnInstance(_participants[0], _resourceName + "0", "TestDB0_2");

    // Resume the controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, _clusterName, "controller_0");
    controller.syncStart();
    Thread.sleep(500L);

    // The disabled instance should not hold any top state replicas (MASTER)
    PropertyKey liveInstanceKey =
        _accessor.keyBuilder().liveInstance(_participants[0].getInstanceName());
    LiveInstance liveInstance = _accessor.getProperty(liveInstanceKey);
    if (liveInstance != null) {
      String sessionId = liveInstance.getSessionId();
      List<CurrentState> currentStates = _accessor.getChildValues(
          _accessor.keyBuilder().currentStates(_participants[0].getInstanceName(), sessionId));
      for (CurrentState currentState : currentStates) {
        for (Map.Entry<String, String> partitionState : currentState.getPartitionStateMap()
            .entrySet()) {
          if (partitionState.getKey().equals("TestDB0_2")) {
            Assert.assertTrue(partitionState.getValue().equals("MASTER"));
          }
        }
      }
    }

    // clean up the cluster
    controller.syncStop();
    for (int i = 0; i < participantCount; i++) {
      _participants[i].syncStop();
    }
    deleteCluster(_clusterName);
  }

  /**
   * Set up the cluster and pause the controller.
   * @param participantCount
   * @throws Exception
   */
  private void setupEnvironment(int participantCount) throws Exception {
    _participants = new MockParticipantManager[participantCount];

    _accessor = new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    setupCluster(_clusterName, participantCount);

    DelayedTransitionBase transition = new DelayedTransitionBase(10L);

    // Start _participants
    for (int i = 0; i < participantCount; i++) {
      _participants[i] =
          new MockParticipantManager(ZK_ADDR, _clusterName, "localhost_" + (12918 + i));
      _participants[i].setTransition(transition);
      _participants[i].syncStart();
    }

    // Start the controller and verify that it is in the best possible state
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, _clusterName, "controller_0");
    controller.syncStart();
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient).build();
    Assert.assertTrue(verifier.verify(3000));

    // Pause the controller
    controller.syncStop();
  }

  /**
   * Set throttle limits only for load balance so that none of them would happen.
   */
  private void setThrottleConfigForLoadBalance() {
    PropertyKey.Builder keyBuilder = _accessor.keyBuilder();

    ClusterConfig clusterConfig = _accessor.getProperty(_accessor.keyBuilder().clusterConfig());
    clusterConfig.setResourcePriorityField("Name");
    List<StateTransitionThrottleConfig> throttleConfigs = new ArrayList<>();

    // Add throttling at cluster-level
    throttleConfigs.add(
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 0));

    // Add throttling at instance level
    throttleConfigs.add(
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, 0));

    clusterConfig.setStateTransitionThrottleConfigs(throttleConfigs);
    _accessor.setProperty(keyBuilder.clusterConfig(), clusterConfig);
  }

  /**
   * Set throttle limits only for recovery balance so that none of them would happen.
   */
  private void setThrottleConfigForRecoveryBalance() {
    PropertyKey.Builder keyBuilder = _accessor.keyBuilder();

    ClusterConfig clusterConfig = _accessor.getProperty(_accessor.keyBuilder().clusterConfig());
    clusterConfig.setResourcePriorityField("Name");
    List<StateTransitionThrottleConfig> throttleConfigs = new ArrayList<>();

    // Add throttling at cluster-level
    throttleConfigs.add(new StateTransitionThrottleConfig(
        StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
        StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 0));

    // Add throttling at instance level
    throttleConfigs.add(new StateTransitionThrottleConfig(
        StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
        StateTransitionThrottleConfig.ThrottleScope.INSTANCE, 0));

    clusterConfig.setStateTransitionThrottleConfigs(throttleConfigs);
    _accessor.setProperty(keyBuilder.clusterConfig(), clusterConfig);
  }

  /**
   * Set up delayed rebalancer and minimum active replica settings to mimic user's use case.
   * @param clusterName
   * @param participantCount
   * @throws Exception
   */
  private void setupCluster(String clusterName, int participantCount) throws Exception {
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant start port
        "localhost", // participant name prefix
        _resourceName, // resource name prefix
        3, // resources
        5, // partitions per resource
        participantCount, // number of nodes
        3, // replicas
        "MasterSlave", IdealState.RebalanceMode.FULL_AUTO, true); // do rebalance

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

  /**
   * Disable select partitions from the first instance to test that these partitions are not subject
   * to throttling.
   */
  private void disablePartitionOnInstance(MockParticipantManager participant, String resourceName,
      String partitionName) {
    String instanceName = participant.getInstanceName();
    PropertyKey key = _accessor.keyBuilder().instanceConfig(instanceName);
    InstanceConfig instanceConfig = _accessor.getProperty(key);
    instanceConfig.setInstanceEnabledForPartition(resourceName, partitionName, false);
    _accessor.setProperty(key, instanceConfig);
  }
}
