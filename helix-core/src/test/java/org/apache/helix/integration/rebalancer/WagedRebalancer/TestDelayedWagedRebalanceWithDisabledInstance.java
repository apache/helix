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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.integration.rebalancer.DelayedAutoRebalancer.TestDelayedAutoRebalanceWithDisabledInstance;
import org.apache.helix.model.ExternalView;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDelayedWagedRebalanceWithDisabledInstance
    extends TestDelayedAutoRebalanceWithDisabledInstance {
  protected ZkHelixClusterVerifier getClusterVerifier() {
    Set<String> dbNames = new HashSet<>();
    int i = 0;
    for (String stateModel : TestStateModels) {
      dbNames.add("Test-DB-" + i++);
    }
    return new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setResources(dbNames)
        .setZkAddr(ZK_ADDR).build();
  }

  // create test DBs, wait it converged and return externalviews
  protected Map<String, ExternalView> createTestDBs(long delayTime) throws InterruptedException {
    Map<String, ExternalView> externalViews = new HashMap<String, ExternalView>();
    int i = 0;
    for (String stateModel : TestStateModels) {
      String db = "Test-DB-" + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, _replica,
          _minActiveReplica);
      _testDBs.add(db);
    }
    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      externalViews.put(db, ev);
    }
    return externalViews;
  }

  @Test(enabled = false)
  @Override
  public void testDelayedPartitionMovement() {
    // Waged Rebalancer takes cluster level delay config only.
  }

  @Test(enabled = false)
  @Override
  public void testDisableDelayRebalanceInResource() {
    // Waged Rebalancer takes cluster level delay config only.
  }
}
