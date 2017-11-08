package org.apache.helix.integration.paticipant;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.builder.ConfigScopeBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

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

public class TestInstanceAutoJoin extends ZkStandAloneCMTestBase {
  String db2 = TEST_DB + "2";

  @Test
  public void testInstanceAutoJoin() throws Exception {
    HelixManager manager = _participants[0];
    HelixDataAccessor accessor = manager.getHelixDataAccessor();

    _setupTool.addResourceToCluster(CLUSTER_NAME, db2, 60, "OnlineOffline", RebalanceMode.FULL_AUTO
        + "");

    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, db2, 1);
    String instance2 = "localhost_279699";
    // StartCMResult result = TestHelper.startDummyProcess(ZK_ADDR, CLUSTER_NAME, instance2);
    MockParticipantManager newParticipant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance2);
    newParticipant.syncStart();

    Thread.sleep(500);
    // Assert.assertFalse(result._thread.isAlive());
    Assert.assertTrue(null == manager.getHelixDataAccessor().getProperty(
        accessor.keyBuilder().liveInstance(instance2)));

    ConfigScope scope = new ConfigScopeBuilder().forCluster(CLUSTER_NAME).build();

    manager.getConfigAccessor().set(scope, ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");

    newParticipant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance2);
    newParticipant.syncStart();

    Thread.sleep(500);
    // Assert.assertTrue(result._thread.isAlive() || result2._thread.isAlive());
    for (int i = 0; i < 20; i++) {
      if (null == manager.getHelixDataAccessor().getProperty(
          accessor.keyBuilder().liveInstance(instance2))) {
        Thread.sleep(100);
      } else
        break;
    }
    Assert.assertTrue(null != manager.getHelixDataAccessor().getProperty(
        accessor.keyBuilder().liveInstance(instance2)));

    newParticipant.syncStop();
  }
}
