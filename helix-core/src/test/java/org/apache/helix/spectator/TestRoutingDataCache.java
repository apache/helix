package org.apache.helix.spectator;

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

import java.util.Map;

import org.apache.helix.HelixConstants;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.MockZkHelixDataAccessor;
import org.apache.helix.model.CurrentState;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRoutingDataCache extends ZkStandAloneCMTestBase {

  @Test()
  public void testUpdateOnNotification() throws Exception {
    MockZkHelixDataAccessor accessor =
        new MockZkHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));

    RoutingDataCache cache =
        new RoutingDataCache("CLUSTER_" + TestHelper.getTestClassName(), PropertyType.EXTERNALVIEW);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 1);

    accessor.clearReadCounters();

    // refresh again should read nothing
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 0);

    accessor.clearReadCounters();
    // refresh again should read nothing as ideal state is same
    cache.notifyDataChange(HelixConstants.ChangeType.EXTERNAL_VIEW);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 0);
  }

  @Test(dependsOnMethods = { "testUpdateOnNotification" })
  public void testSelectiveUpdates()
      throws Exception {
    MockZkHelixDataAccessor accessor =
        new MockZkHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));

    RoutingDataCache cache =
        new RoutingDataCache("CLUSTER_" + TestHelper.getTestClassName(), PropertyType.EXTERNALVIEW);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 1);

    accessor.clearReadCounters();

    // refresh again should read nothing
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 0);

    // refresh again should read nothing
    cache.notifyDataChange(HelixConstants.ChangeType.EXTERNAL_VIEW);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 0);

    // add new resources
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "TestDB_1", 1, STATE_MODEL);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, "TestDB_1", _replica);

    Thread.sleep(100);
    ZkHelixClusterVerifier _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    accessor.clearReadCounters();

    // refresh again should read only new current states and new idealstate
    cache.notifyDataChange(HelixConstants.ChangeType.EXTERNAL_VIEW);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 1);

    // Add more resources
    accessor.clearReadCounters();

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "TestDB_2", 1, STATE_MODEL);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, "TestDB_2", _replica);
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "TestDB_3", 1, STATE_MODEL);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, "TestDB_3", _replica);

    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Totally four resources. Two of them are newly added.
    cache.notifyDataChange(HelixConstants.ChangeType.EXTERNAL_VIEW);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 2);

    // update one resource
    accessor.clearReadCounters();

    _gSetupTool.getClusterManagementTool().enableResource(CLUSTER_NAME, "TestDB_2", false);

    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    cache.notifyDataChange(HelixConstants.ChangeType.EXTERNAL_VIEW);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 1);
  }

  @Test
  public void testCurrentStatesSelectiveUpdate() {
    String clusterName = "CLUSTER_" + TestHelper.getTestClassName();
    MockZkHelixDataAccessor accessor =
        new MockZkHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<>(_gZkClient));
    RoutingDataCache cache = new RoutingDataCache(clusterName, PropertyType.CURRENTSTATES);

    // Empty current states map before refreshing.
    Assert.assertTrue(cache.getCurrentStatesMap().isEmpty());

    // 1. Initial cache refresh.
    cache.refresh(accessor);
    Map<String, Map<String, Map<String, CurrentState>>> currentStatesV1 = cache.getCurrentStatesMap();

    // Current states map is not empty and size equals to number of live instances.
    Assert.assertFalse(currentStatesV1.isEmpty());
    Assert.assertEquals(currentStatesV1.size(), _participants.length);

    // 2. Without any change, refresh routing data cache.
    cache.refresh(accessor);
    // Because of no current states change, current states cache doesn't refresh.
    Assert.assertEquals(cache.getCurrentStatesMap(), currentStatesV1);

    // 3. Stop one participant to make live instance change and refresh routing data cache.
    _participants[0].syncStop();
    cache.notifyDataChange(HelixConstants.ChangeType.LIVE_INSTANCE);
    cache.refresh(accessor);
    Map<String, Map<String, Map<String, CurrentState>>> currentStatesV2 = cache.getCurrentStatesMap();

    // Current states cache should refresh and change.
    Assert.assertFalse(currentStatesV2.isEmpty());
    Assert.assertEquals(currentStatesV2.size(), _participants.length - 1);
    Assert.assertFalse(currentStatesV1.equals(currentStatesV2));

    cache.refresh(accessor);

    // No change.
    Assert.assertEquals(cache.getCurrentStatesMap(), currentStatesV2);
  }
}
