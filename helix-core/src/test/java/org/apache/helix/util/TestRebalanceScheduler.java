package org.apache.helix.util;

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

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestRebalanceScheduler extends ZkTestBase {
  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private HelixManager _manager;
  private ConfigAccessor _configAccessor;
  private final int NUM_ATTEMPTS = 10;

  @BeforeClass
  public void beforeClass() throws Exception {
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _manager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "Test", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
    _configAccessor = new ConfigAccessor(_gZkClient);
  }

  @Test
  public void testInvokeRebalanceAndInvokeRebalanceForResource() {
    String resourceName = "ResourceToInvoke";
    _gSetupTool.getClusterManagementTool()
        .addResource(CLUSTER_NAME, resourceName, 5, MasterSlaveSMD.name);
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, resourceName);

    // Add listfields for ResourceConfig
    ResourceConfig resourceConfig = new ResourceConfig(resourceName);
    resourceConfig.setPreferenceLists(Collections.singletonMap("0", Arrays.asList("1", "2", "3")));
    _configAccessor.setResourceConfig(CLUSTER_NAME, resourceName, resourceConfig);

    int i = 0;
    while (i++ < NUM_ATTEMPTS) {
      RebalanceScheduler.invokeRebalance(_manager.getHelixDataAccessor(), resourceName);
      RebalanceScheduler
          .invokeRebalanceForResourceConfig(_manager.getHelixDataAccessor(), resourceName);
    }

    IdealState newIdealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, resourceName);
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    ResourceConfig newResourceConfig =
        accessor.getProperty(accessor.keyBuilder().resourceConfig(resourceName));

    // Starting version should be 0 and finally the version should be same as NUM_ATTEMPTS
    Assert.assertTrue(idealState.getRecord().equals(newIdealState.getRecord()));
    Assert.assertEquals(idealState.getStat().getVersion(), 0);
    Assert.assertEquals(newIdealState.getStat().getVersion(), NUM_ATTEMPTS);

    Assert.assertTrue(resourceConfig.getRecord().equals(newResourceConfig.getRecord()));
    Assert.assertEquals(
        resourceConfig.getStat().getVersion(), 0);
    Assert.assertEquals(newResourceConfig.getStat().getVersion(), NUM_ATTEMPTS);

  }
}
