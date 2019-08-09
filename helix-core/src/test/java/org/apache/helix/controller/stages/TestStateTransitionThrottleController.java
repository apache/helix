package org.apache.helix.controller.stages;

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

import static org.apache.helix.api.config.StateTransitionThrottleConfig.RebalanceType.ANY;
import static org.apache.helix.api.config.StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE;
import static org.apache.helix.api.config.StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.model.ClusterConfig;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestStateTransitionThrottleController {
  private static final String INSTANCE = "instance0";
  private static final String RESOURCE = "db0";
  private static final List<StateTransitionThrottleConfig.RebalanceType> VALID_REBALANCE_TYPES =
      ImmutableList.of(LOAD_BALANCE, RECOVERY_BALANCE, ANY);

  @Test(description = "When cluster level ANY throttle config is set")
  public void testChargeClusterWhenANYClusterLevelThrottleConfig() {
    int maxNumberOfST = 1;
    ClusterConfig clusterConfig = new ClusterConfig("config");
    clusterConfig
        .setStateTransitionThrottleConfigs(ImmutableList.of(new StateTransitionThrottleConfig(ANY,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, maxNumberOfST)));

    StateTransitionThrottleControllerAccessor controller =
        new StateTransitionThrottleControllerAccessor(RESOURCE, INSTANCE, clusterConfig);
    Assert.assertTrue(controller.isThrottleEnabled());

    for (StateTransitionThrottleConfig.RebalanceType rebalanceType : VALID_REBALANCE_TYPES) {
      controller.chargeCluster(rebalanceType);
      for (StateTransitionThrottleConfig.RebalanceType type : VALID_REBALANCE_TYPES) {
        Assert.assertTrue(controller.shouldThrottleForCluster(type));
        Assert.assertTrue(controller.shouldThrottleForInstance(type, INSTANCE));
        Assert.assertTrue(controller.shouldThrottleForInstance(type, RESOURCE));
      }
      // reset controller
      controller = new StateTransitionThrottleControllerAccessor(RESOURCE, INSTANCE, clusterConfig);
    }
  }

  @Test(description = "When cluster throttle is config of LOAD_BALANCE/RECOVERY_BALANCE, no ANY type")
  public void testChargeCluster_OnlySetClusterSpecificType() {
    int maxNumberOfST = 1;
    ClusterConfig clusterConfig = new ClusterConfig("config");
    clusterConfig.setStateTransitionThrottleConfigs(ImmutableList.of(
        new StateTransitionThrottleConfig(RECOVERY_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, maxNumberOfST),
        new StateTransitionThrottleConfig(LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, maxNumberOfST)));

    StateTransitionThrottleControllerAccessor controller =
        new StateTransitionThrottleControllerAccessor(RESOURCE, INSTANCE, clusterConfig);

    Assert.assertTrue(controller.isThrottleEnabled());

    controller.chargeCluster(ANY);
    Assert.assertEquals(controller.getClusterLevelQuota(RECOVERY_BALANCE), 1);
    Assert.assertEquals(controller.getClusterLevelQuota(LOAD_BALANCE), 1);
    Assert.assertEquals(controller.getClusterLevelQuota(ANY), 0);

    VALID_REBALANCE_TYPES.forEach(controller::chargeCluster);
    for (StateTransitionThrottleConfig.RebalanceType rebalanceType : ImmutableList.of(LOAD_BALANCE,
        RECOVERY_BALANCE)) {
      Assert.assertTrue(controller.shouldThrottleForCluster(rebalanceType));
      Assert.assertTrue(controller.shouldThrottleForInstance(rebalanceType, INSTANCE));
      Assert.assertTrue(controller.shouldThrottleForResource(rebalanceType, RESOURCE));
    }
  }

  @DataProvider
  public static Object[][] mixedConfigurations() {
    // TODO: add more mixed configuration setting when refactoring the controller logic
    return new Object[][] {
        {
            10, 9, 8, 7, 6, 5, 4, 3, 2
        }
    };
  }

  @Test(dataProvider = "mixedConfigurations")
  public void testChargeClusterWithMixedThrottleConfig(int anyClusterLevelQuota,
      int loadClusterLevelQuota, int recoveryClusterLevelQuota, int anyInstanceLevelQuota,
      int loadInstanceLevelQuota, int recoveryInstanceLevelQuota, int anyResourceLevelQuota,
      int loadResourceLevelQuota, int recoveryResourceLevelQuota) {
    List<StateTransitionThrottleConfig> stateTransitionThrottleConfigs = Arrays.asList(
        new StateTransitionThrottleConfig(ANY, StateTransitionThrottleConfig.ThrottleScope.CLUSTER,
            anyClusterLevelQuota),
        new StateTransitionThrottleConfig(RECOVERY_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, recoveryClusterLevelQuota),
        new StateTransitionThrottleConfig(LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, loadClusterLevelQuota),
        new StateTransitionThrottleConfig(ANY, StateTransitionThrottleConfig.ThrottleScope.INSTANCE,
            anyInstanceLevelQuota),
        new StateTransitionThrottleConfig(RECOVERY_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, recoveryInstanceLevelQuota),
        new StateTransitionThrottleConfig(LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, loadInstanceLevelQuota),
        new StateTransitionThrottleConfig(ANY, StateTransitionThrottleConfig.ThrottleScope.RESOURCE,
            anyResourceLevelQuota),
        new StateTransitionThrottleConfig(RECOVERY_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.RESOURCE, recoveryResourceLevelQuota),
        new StateTransitionThrottleConfig(LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.RESOURCE, loadResourceLevelQuota));
    ClusterConfig clusterConfig = new ClusterConfig("config");
    clusterConfig.setStateTransitionThrottleConfigs(stateTransitionThrottleConfigs);

    StateTransitionThrottleControllerAccessor controller =
        new StateTransitionThrottleControllerAccessor(RESOURCE, INSTANCE, clusterConfig);

    Assert.assertTrue(controller.isThrottleEnabled());

    // verify behavior after charging cluster
    controller.chargeCluster(ANY);
    Assert.assertEquals(controller.getClusterLevelQuota(ANY), anyClusterLevelQuota - 1);
    controller.chargeCluster(RECOVERY_BALANCE);
    Assert.assertEquals(controller.getClusterLevelQuota(RECOVERY_BALANCE),
        recoveryClusterLevelQuota - 1);
    controller.chargeCluster(LOAD_BALANCE);
    Assert.assertEquals(controller.getClusterLevelQuota(LOAD_BALANCE), loadClusterLevelQuota - 1);

    // verify behavior after charging instance
    controller.chargeInstance(ANY, INSTANCE);
    Assert.assertEquals(controller.getInstanceLevelQuota(ANY, INSTANCE), anyInstanceLevelQuota - 1);
    controller.chargeInstance(RECOVERY_BALANCE, INSTANCE);
    Assert.assertEquals(controller.getInstanceLevelQuota(RECOVERY_BALANCE, INSTANCE),
        recoveryInstanceLevelQuota - 1);
    controller.chargeInstance(LOAD_BALANCE, INSTANCE);
    Assert.assertEquals(controller.getInstanceLevelQuota(LOAD_BALANCE, INSTANCE),
        loadInstanceLevelQuota - 1);

    // verify behavior after charging resource
    controller.chargeResource(ANY, RESOURCE);
    Assert.assertEquals(controller.getResourceLevelQuota(ANY, RESOURCE), anyResourceLevelQuota - 1);
    controller.chargeResource(RECOVERY_BALANCE, RESOURCE);
    Assert.assertEquals(controller.getResourceLevelQuota(RECOVERY_BALANCE, RESOURCE),
        recoveryResourceLevelQuota - 1);
    controller.chargeResource(LOAD_BALANCE, RESOURCE);
    Assert.assertEquals(controller.getResourceLevelQuota(LOAD_BALANCE, RESOURCE),
        loadResourceLevelQuota - 1);
  }

  // The inner class just to fetch the protected fields of {@link StateTransitionThrottleController}
  private static class StateTransitionThrottleControllerAccessor
      extends StateTransitionThrottleController {
    StateTransitionThrottleControllerAccessor(String resource, String liveInstance,
        ClusterConfig clusterConfig) {
      super(ImmutableSet.of(resource), clusterConfig, ImmutableSet.of(liveInstance));
    }

    long getClusterLevelQuota(StateTransitionThrottleConfig.RebalanceType rebalanceType) {
      return _pendingTransitionAllowedInCluster.getOrDefault(rebalanceType, 0L);
    }

    long getResourceLevelQuota(StateTransitionThrottleConfig.RebalanceType rebalanceType,
        String resource) {
      return _pendingTransitionAllowedPerResource.getOrDefault(resource, Collections.emptyMap())
          .getOrDefault(rebalanceType, 0L);
    }

    long getInstanceLevelQuota(StateTransitionThrottleConfig.RebalanceType rebalanceType,
        String instance) {
      return _pendingTransitionAllowedPerInstance.getOrDefault(instance, Collections.emptyMap())
          .getOrDefault(rebalanceType, 0L);
    }
  }
}
