package org.apache.helix.integration.rebalancer.WagedRebalancer;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.TestHelper;
import org.apache.helix.integration.rebalancer.DelayedAutoRebalancer.TestDelayedAutoRebalanceWithRackaware;
import org.apache.helix.model.ExternalView;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Inherit TestDelayedAutoRebalanceWithRackaware to ensure the test logic is the same.
 */
public class TestDelayedWagedRebalanceWithRackaware extends TestDelayedAutoRebalanceWithRackaware {
  // create test DBs, wait it converged and return externalviews
  protected Map<String, ExternalView> createTestDBs(long delayTime)
      throws InterruptedException {
    Map<String, ExternalView> externalViews = new HashMap<>();
    int i = 0;
    for (String stateModel : TestStateModels) {
      String db = "Test-DB-" + TestHelper.getTestMethodName() + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica,
          _minActiveReplica);
      _testDBs.add(db);
    }
    Thread.sleep(DEFAULT_REBALANCE_PROCESSING_WAIT_TIME);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      externalViews.put(db, ev);
    }
    return externalViews;
  }

  @Test
  public void testDelayedPartitionMovement() {
    // Waged Rebalancer takes cluster level delay config only. Skip this test.
  }

  @Test
  public void testDisableDelayRebalanceInResource() {
    // Waged Rebalancer takes cluster level delay config only. Skip this test.
  }

  @Test(dependsOnMethods = {"testDelayedPartitionMovement"})
  public void testDelayedPartitionMovementWithClusterConfigedDelay()
      throws Exception {
    super.testDelayedPartitionMovementWithClusterConfigedDelay();
  }

  @Test(dependsOnMethods = {"testDelayedPartitionMovementWithClusterConfigedDelay"})
  public void testMinimalActiveReplicaMaintain()
      throws Exception {
    super.testMinimalActiveReplicaMaintain();
  }

  @Test(dependsOnMethods = {"testMinimalActiveReplicaMaintain"})
  public void testPartitionMovementAfterDelayTime()
      throws Exception {
    super.testPartitionMovementAfterDelayTime();
  }

  @Test(dependsOnMethods = {"testDisableDelayRebalanceInResource"})
  public void testDisableDelayRebalanceInCluster()
      throws Exception {
    super.testDisableDelayRebalanceInCluster();
  }

  @Test(dependsOnMethods = {"testDisableDelayRebalanceInCluster"})
  public void testDisableDelayRebalanceInInstance()
      throws Exception {
    super.testDisableDelayRebalanceInInstance();
  }
}
