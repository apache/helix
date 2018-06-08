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

import org.apache.helix.*;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.participant.ErrTransition;
import org.apache.helix.mock.participant.SleepTransition;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;

public class TestStateTransitionThrottle extends ZkTestBase {
  int participantCount = 4;
  String resourceName = "TestDB0";
  String resourceNamePrefix = "TestDB";

  @Test
  public void testTransitionThrottleOnRecoveryPartition() throws Exception {
    String clusterName = getShortClassName() + "testRecoveryPartition";
    MockParticipantManager[] participants = new MockParticipantManager[participantCount];

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    final ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    setupCluster(clusterName, accessor);

    // start partial participants
    for (int i = 0; i < participantCount - 1; i++) {
      participants[i] =
          new MockParticipantManager(ZK_ADDR, clusterName, "localhost_" + (12918 + i));
      if (i == 0) {
        // One participant 0, delay processing partition 0 transition
        final String delayedPartitionName = resourceName + "_0";
        participants[i].setTransition(new SleepTransition(99999999) {
          @Override
          public void doTransition(Message message, NotificationContext context)
              throws InterruptedException {
            String partition = message.getPartitionName();
            if (partition.equals(delayedPartitionName)) {
              super.doTransition(message, context);
            }
          }
        });
      }
      participants[i].syncStart();
    }

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient).build();
    // Won't match, since there is pending transition
    Assert.assertFalse(verifier.verify(3000));

    participants[participantCount - 1] = new MockParticipantManager(ZK_ADDR, clusterName,
        "localhost_" + (12918 + participantCount - 1));
    participants[participantCount - 1].syncStart();

    // Load balance transition won't be scheduled since there is pending recovery balance transition
    Assert.assertFalse(
        pollForPartitionAssignment(accessor, participants[participantCount - 1], resourceName,
            5000));

    // Stop participant, so blocking transition is removed.
    participants[0].syncStop();
    Assert.assertTrue(
        pollForPartitionAssignment(accessor, participants[participantCount - 1], resourceName,
            5000));

    // clean up
    controller.syncStop();
    for (int i = 0; i < participantCount; i++) {
      participants[i].syncStop();
    }
    _gSetupTool.deleteCluster(clusterName);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testTransitionThrottleOnErrorPartition() throws Exception {
    String clusterName = getShortClassName() + "testMaxErrorPartition";
    MockParticipantManager[] participants = new MockParticipantManager[participantCount];

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    final ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    setupCluster(clusterName, accessor);

    // Set throttle config to enable throttling
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setResourcePriorityField("Name");
    List<StateTransitionThrottleConfig> throttleConfigs = new ArrayList<>();
    throttleConfigs.add(
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 100));
    throttleConfigs.add(new StateTransitionThrottleConfig(
        StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
        StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 100));
    clusterConfig.setStateTransitionThrottleConfigs(throttleConfigs);
    accessor.setProperty(keyBuilder.clusterConfig(), clusterConfig);

    // set one partition to be always Error, so load balance won't be triggered
    Map<String, Set<String>> errPartitions = new HashMap<>();
    errPartitions.put("OFFLINE-SLAVE", TestHelper.setOf(resourceName + "_0"));

    // start part of participants
    for (int i = 0; i < participantCount - 1; i++) {
      participants[i] =
          new MockParticipantManager(ZK_ADDR, clusterName, "localhost_" + (12918 + i));
      if (i == 0) {
        participants[i].setTransition(new ErrTransition(errPartitions));
      }
      participants[i].syncStart();
    }

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient).build();
    Assert.assertTrue(verifier.verify(3000));

    // Adding one more participant.
    participants[participantCount - 1] = new MockParticipantManager(ZK_ADDR, clusterName,
        "localhost_" + (12918 + participantCount - 1));
    participants[participantCount - 1].syncStart();
    // Since error partition exists, no load balance transition will be done
    Assert.assertFalse(
        pollForPartitionAssignment(accessor, participants[participantCount - 1], resourceName,
            5000));

    // Update cluster config to tolerate error partition, so load balance transition will be done
    clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setErrorPartitionThresholdForLoadBalance(1);
    accessor.setProperty(keyBuilder.clusterConfig(), clusterConfig);
    _gSetupTool.rebalanceResource(clusterName, resourceName, 3);

    Assert.assertTrue(
        pollForPartitionAssignment(accessor, participants[participantCount - 1], resourceName,
            3000));

    // clean up
    controller.syncStop();
    for (int i = 0; i < participantCount; i++) {
      participants[i].syncStop();
    }
    _gSetupTool.deleteCluster(clusterName);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  private void setupCluster(String clusterName, ZKHelixDataAccessor accessor) throws Exception {
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant start port
        "localhost", // participant name prefix
        resourceNamePrefix, // resource name prefix
        1, // resources
        15, // partitions per resource
        participantCount, // number of nodes
        3, // replicas
        "MasterSlave", IdealState.RebalanceMode.FULL_AUTO, true); // do rebalance

    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setResourcePriorityField("Name");
    List<StateTransitionThrottleConfig> throttleConfigs = new ArrayList<>();
    throttleConfigs.add(
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 100));
    throttleConfigs.add(new StateTransitionThrottleConfig(
        StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
        StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 100));
    clusterConfig.setStateTransitionThrottleConfigs(throttleConfigs);
    accessor.setProperty(keyBuilder.clusterConfig(), clusterConfig);

    _gSetupTool.deleteCluster(clusterName);
  }

  private static boolean pollForPartitionAssignment(final HelixDataAccessor accessor,
      final MockParticipantManager participant, final String resourceName, final int timeout)
      throws Exception {
    return TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();
        PropertyKey partitionStatusKey = keyBuilder
            .currentState(participant.getInstanceName(), participant.getSessionId(), resourceName);
        CurrentState currentState = accessor.getProperty(partitionStatusKey);
        return currentState != null && !currentState.getPartitionStateMap().isEmpty();
      }
    }, timeout);
  }
}
