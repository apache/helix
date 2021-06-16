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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.TestHelper;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.api.status.ClusterManagementModeRequest;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.dataproviders.ManagementControllerDataProvider;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterStatus;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestManagementModeStage extends ZkTestBase {
  HelixManager _manager;
  HelixDataAccessor _accessor;
  String _clusterName;

  @BeforeClass
  public void beforeClass() {
    _clusterName = "CLUSTER_" + TestHelper.getTestClassName();
    _accessor = new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    _manager = new DummyClusterManager(_clusterName, _accessor);
  }

  @AfterClass
  public void afterClass() {
    deleteLiveInstances(_clusterName);
    deleteCluster(_clusterName);
  }

  @Test
  public void testClusterFreezeStatus() throws Exception {
    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    setupIdealState(_clusterName, new int[]{0, 1}, new String[]{"TestDB"}, 1, 2);
    setupLiveInstances(_clusterName, new int[]{0, 1});
    setupStateModel(_clusterName);

    ClusterEvent event = new ClusterEvent(_clusterName, ClusterEventType.Unknown);
    ManagementControllerDataProvider cache = new ManagementControllerDataProvider(_clusterName,
        Pipeline.Type.MANAGEMENT_MODE.name());
    event.addAttribute(AttributeName.helixmanager.name(), _manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    // Freeze cluster
    ClusterManagementModeRequest request = ClusterManagementModeRequest.newBuilder()
        .withClusterName(_clusterName)
        .withMode(ClusterManagementMode.Type.CLUSTER_PAUSE)
        .withReason("test")
        .build();
    _gSetupTool.getClusterManagementTool().setClusterManagementMode(request);

    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());
    runPipeline(event, dataRefresh, false);
    new ManagementModeStage().process(event);

    ClusterStatus clusterStatus = _accessor.getProperty(_accessor.keyBuilder().clusterStatus());
    Assert.assertEquals(clusterStatus.getManagementMode(), ClusterManagementMode.Type.CLUSTER_PAUSE);

    // Unfreeze cluster
    request = ClusterManagementModeRequest.newBuilder()
        .withClusterName(_clusterName)
        .withMode(ClusterManagementMode.Type.NORMAL)
        .withReason("test")
        .build();
    _gSetupTool.getClusterManagementTool().setClusterManagementMode(request);
    runPipeline(event, dataRefresh, false);

    try {
      new ManagementModeStage().process(event);
    } catch (HelixException expected) {
      Assert.assertTrue(expected.getMessage()
          .startsWith("Failed to switch management mode pipeline, enabled=false"));
    }

    clusterStatus = _accessor.getProperty(_accessor.keyBuilder().clusterStatus());
    Assert.assertEquals(clusterStatus.getManagementMode(), ClusterManagementMode.Type.NORMAL);
  }
}
