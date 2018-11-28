package org.apache.helix.integration.controller;

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

import org.apache.helix.HelixConstants;
import org.apache.helix.TestHelper;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSkipBestPossibleCalculation extends ZkStandAloneCMTestBase {

  @Test()
  public void test() throws Exception {
    int numResource = 5;
    for (int i = 0; i < numResource; i++) {
      String dbName = "TestDB_" + i;
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, dbName, _PARTITIONS, STATE_MODEL,
          IdealState.RebalanceMode.CUSTOMIZED.name());
      _gSetupTool.rebalanceResource(CLUSTER_NAME, dbName, 3);
    }

    ResourceControllerDataProvider cache =
        new ResourceControllerDataProvider("CLUSTER_" + TestHelper.getTestClassName());
    cache.refresh(_manager.getHelixDataAccessor());

    ClusterEvent event = new ClusterEvent(CLUSTER_NAME, ClusterEventType.IdealStateChange);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    runStage(_manager, event, new ResourceComputationStage());
    runStage(_manager, event, new CurrentStateComputationStage());

    Assert.assertEquals(cache.getCachedResourceAssignments().size(), 0);
    runStage(_manager, event, new BestPossibleStateCalcStage());
    Assert.assertEquals(cache.getCachedResourceAssignments().size(), numResource);

    cache.notifyDataChange(HelixConstants.ChangeType.INSTANCE_CONFIG);
    cache.refresh(_manager.getHelixDataAccessor());
    Assert.assertEquals(cache.getCachedResourceAssignments().size(), 0);
    runStage(_manager, event, new BestPossibleStateCalcStage());
    Assert.assertEquals(cache.getCachedResourceAssignments().size(), numResource);

    cache.notifyDataChange(HelixConstants.ChangeType.IDEAL_STATE);
    cache.refresh(_manager.getHelixDataAccessor());
    Assert.assertEquals(cache.getCachedResourceAssignments().size(), 0);
    runStage(_manager, event, new BestPossibleStateCalcStage());
    Assert.assertEquals(cache.getCachedResourceAssignments().size(), numResource);

    cache.notifyDataChange(HelixConstants.ChangeType.LIVE_INSTANCE);
    cache.refresh(_manager.getHelixDataAccessor());
    Assert.assertEquals(cache.getCachedResourceAssignments().size(), 0);
    runStage(_manager, event, new BestPossibleStateCalcStage());
    Assert.assertEquals(cache.getCachedResourceAssignments().size(), numResource);

    cache.requireFullRefresh();
    cache.refresh(_manager.getHelixDataAccessor());
    Assert.assertEquals(cache.getCachedResourceAssignments().size(), 0);
    runStage(_manager, event, new BestPossibleStateCalcStage());
    Assert.assertEquals(cache.getCachedResourceAssignments().size(), numResource);

    cache.notifyDataChange(HelixConstants.ChangeType.CURRENT_STATE);
    cache.refresh(_manager.getHelixDataAccessor());
    Assert.assertEquals(cache.getCachedResourceAssignments().size(), numResource);

    cache.notifyDataChange(HelixConstants.ChangeType.RESOURCE_CONFIG);
    cache.refresh(_manager.getHelixDataAccessor());
    Assert.assertEquals(cache.getCachedResourceAssignments().size(), 0);
  }
}
