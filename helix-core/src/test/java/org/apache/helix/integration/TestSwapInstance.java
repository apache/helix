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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSwapInstance extends ZkStandAloneCMTestBase {
  @Test
  public void testSwapInstance() throws Exception {
    HelixManager manager = _controller;
    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();

    // Create semi auto resource
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "db-semi", 64, STATE_MODEL,
        IdealState.RebalanceMode.SEMI_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, "db-semi", _replica);

    // Create customized resource
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "db-customized", 64, STATE_MODEL,
        IdealState.RebalanceMode.CUSTOMIZED.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, "db-customized", _replica);

    // Create full-auto resource
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "db-fa", 64, STATE_MODEL,
        IdealState.RebalanceMode.FULL_AUTO.name(), RebalanceStrategy.DEFAULT_REBALANCE_STRATEGY);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, "db-fa", _replica);

    // Wait for cluster converge
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Get ideal states before swap
    IdealState semiIS = dataAccessor.getProperty(dataAccessor.keyBuilder().idealStates("db-semi"));
    IdealState customizedIS =
        dataAccessor.getProperty(dataAccessor.keyBuilder().idealStates("db-customized"));
    IdealState faIs =
        dataAccessor.getProperty(dataAccessor.keyBuilder().idealStates("db-fa"));

    String oldInstanceName = String.format("%s_%s", PARTICIPANT_PREFIX, START_PORT);
    String newInstanceName = String.format("%s_%s", PARTICIPANT_PREFIX, 66666);

    try {
      _gSetupTool.swapInstance(CLUSTER_NAME, oldInstanceName, newInstanceName);
      Assert.fail("Cannot swap as new instance is not added to cluster yet");
    } catch (Exception e) {
      // OK - new instance not added to cluster yet
    }

    // Add new instance to cluster
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, newInstanceName);

    try {
      _gSetupTool.swapInstance(CLUSTER_NAME, oldInstanceName, newInstanceName);
      Assert.fail("Cannot swap as old instance is still alive");
    } catch (Exception e) {
      // OK - old instance still alive
    }

    // Stop old instance
    _participants[0].syncStop();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    try {
      _gSetupTool.swapInstance(CLUSTER_NAME, oldInstanceName, newInstanceName);
      Assert.fail("Cannot swap as old instance is still enabled");
    } catch (Exception e) {
      // OK - old instance still alive
    }

    // disable old instance
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, oldInstanceName, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // We can swap now
    _gSetupTool.swapInstance(CLUSTER_NAME, oldInstanceName, newInstanceName);

    // verify cluster
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    verifySwapInstance(dataAccessor, "db-semi", semiIS, oldInstanceName, newInstanceName, false);
    verifySwapInstance(dataAccessor, "db-customized", customizedIS, oldInstanceName, newInstanceName,
        false);
    verifySwapInstance(dataAccessor, "db-fa", faIs, oldInstanceName, newInstanceName, true);

    // Verify idempotency
    _gSetupTool.swapInstance(CLUSTER_NAME, oldInstanceName, newInstanceName);
    verifySwapInstance(dataAccessor, "db-semi", semiIS, oldInstanceName, newInstanceName, false);
    verifySwapInstance(dataAccessor, "db-customized", customizedIS, oldInstanceName, newInstanceName,
        false);
    verifySwapInstance(dataAccessor, "db-fa", faIs, oldInstanceName, newInstanceName, true);

  }

  private void verifySwapInstance(HelixDataAccessor dataAccessor, String resourceName,
      IdealState oldIs, String oldInstance, String newInstance, boolean isFullAuto) {
    IdealState newIs = dataAccessor.getProperty(dataAccessor.keyBuilder().idealStates(resourceName));
    if (isFullAuto) {
      // Full auto resource should not contain new instance as it's not live yet
      for (String key : newIs.getRecord().getMapFields().keySet()) {
        Assert.assertFalse(newIs.getRecord().getMapField(key).keySet().contains(newInstance));
      }

      for (String key : newIs.getRecord().getListFields().keySet()) {
        Assert.assertFalse(newIs.getRecord().getListField(key).contains(newInstance));
      }
    } else {
      verifyIdealStateWithSwappedInstance(oldIs, newIs, oldInstance, newInstance);
    }
  }

  private void verifyIdealStateWithSwappedInstance(IdealState oldIs, IdealState newIs,
      String oldInstance, String newInstance) {

    // Verify map fields
    for (String key : oldIs.getRecord().getMapFields().keySet()) {
      for (String host : oldIs.getRecord().getMapField(key).keySet()) {
        if (host.equals(oldInstance)) {
          Assert.assertTrue(oldIs.getRecord().getMapField(key).get(oldInstance)
              .equals(newIs.getRecord().getMapField(key).get(newInstance)));
        } else {
          Assert.assertTrue(oldIs.getRecord().getMapField(key).get(host)
              .equals(newIs.getRecord().getMapField(key).get(host)));
        }
      }
    }

    // verify list fields
    for (String key : oldIs.getRecord().getListFields().keySet()) {
      Assert.assertEquals(oldIs.getRecord().getListField(key).size(), newIs.getRecord()
          .getListField(key).size());
      for (int i = 0; i < oldIs.getRecord().getListField(key).size(); i++) {
        String host = oldIs.getRecord().getListField(key).get(i);
        String newHost = newIs.getRecord().getListField(key).get(i);
        if (host.equals(oldInstance)) {
          Assert.assertTrue(newHost.equals(newInstance));
        } else {
          Assert.assertTrue(host.equals(newHost));
        }
      }
    }
  }

}
