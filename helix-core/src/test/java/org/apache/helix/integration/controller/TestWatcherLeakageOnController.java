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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;
import java.util.Map;

import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestWatcherLeakageOnController extends ZkTestBase {
  private final String CLASS_NAME = getShortClassName();
  private final String TEST_RESOURCE = "TestResource";
  private final String CLUSTER_NAME = "TestCluster-" + CLASS_NAME;
  private ZkHelixClusterVerifier _clusterVerifier;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
            .build();
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, "TestInstance");
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, TEST_RESOURCE, 10, "MasterSlave");
  }

  @AfterClass
  public void afterClass() {
    deleteCluster(CLUSTER_NAME);
  }

  @Test
  public void testWatcherOnResourceDeletion() throws Exception {
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, "TestController");
    controller.syncStart();
    Map<String, List<String>> zkWatches = ZkTestHelper.getZkWatch(controller.getZkClient());

    List<String> dataWatchesBefore = zkWatches.get("dataWatches");

    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, TEST_RESOURCE);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    zkWatches = ZkTestHelper.getZkWatch(controller.getZkClient());
    List<String> dataWatchesAfter = zkWatches.get("dataWatches");

    Assert.assertEquals(dataWatchesBefore.size() - dataWatchesAfter.size(), 1);
    dataWatchesBefore.removeAll(dataWatchesAfter);
    // The data watch on [/TestCluster-TestWatcherLeakageOnController/IDEALSTATES/TestResource] should be removed
    Assert.assertTrue(dataWatchesBefore.get(0).contains(TEST_RESOURCE));

    controller.syncStop();
  }

  @Test
  public void testWatcherOnResourceAddition() throws Exception {
    String tmpResource = "tmpResource";
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, "TestController");
    controller.syncStart();
    Map<String, List<String>> zkWatches = ZkTestHelper.getZkWatch(controller.getZkClient());

    List<String> dataWatchesBefore = zkWatches.get("dataWatches");

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, tmpResource, 10, "MasterSlave");
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    zkWatches = ZkTestHelper.getZkWatch(controller.getZkClient());
    List<String> dataWatchesAfter = zkWatches.get("dataWatches");

    Assert.assertEquals(dataWatchesAfter.size() - dataWatchesBefore.size(), 1);
    dataWatchesAfter.removeAll(dataWatchesBefore);
    // The data watch on [/TestCluster-TestWatcherLeakageOnController/IDEALSTATES/tmpResource] should be added
    Assert.assertTrue(dataWatchesAfter.get(0).contains(tmpResource));

    controller.syncStop();
  }
}
