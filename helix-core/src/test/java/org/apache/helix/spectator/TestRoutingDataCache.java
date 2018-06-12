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

import org.apache.helix.HelixConstants;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.MockZkHelixDataAccessor;
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
}
