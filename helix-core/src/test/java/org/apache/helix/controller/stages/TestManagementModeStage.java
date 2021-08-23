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

import java.util.List;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.api.status.ClusterManagementModeRequest;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.dataproviders.ManagementControllerDataProvider;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterStatus;
import org.apache.helix.model.ControllerHistory;
import org.apache.helix.model.LiveInstance;
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
    List<LiveInstance> liveInstances = setupLiveInstances(_clusterName, new int[]{0, 1});
    setupStateModel(_clusterName);

    ClusterEvent event = new ClusterEvent(_clusterName, ClusterEventType.Unknown);
    ManagementControllerDataProvider cache = new ManagementControllerDataProvider(_clusterName,
        Pipeline.Type.MANAGEMENT_MODE.name());
    event.addAttribute(AttributeName.helixmanager.name(), _manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    // Freeze cluster
    ClusterManagementModeRequest request = ClusterManagementModeRequest.newBuilder()
        .withClusterName(_clusterName)
        .withMode(ClusterManagementMode.Type.CLUSTER_FREEZE)
        .withReason("test")
        .build();
    _gSetupTool.getClusterManagementTool().setClusterManagementMode(request);

    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());
    dataRefresh.addStage(new ResourceComputationStage());
    dataRefresh.addStage(new CurrentStateComputationStage());
    runPipeline(event, dataRefresh, false);
    ManagementModeStage managementModeStage = new ManagementModeStage();
    managementModeStage.process(event);

    // In frozen mode
    ClusterStatus clusterStatus = _accessor.getProperty(_accessor.keyBuilder().clusterStatus());
    Assert.assertEquals(clusterStatus.getManagementMode(), ClusterManagementMode.Type.CLUSTER_FREEZE);

    ControllerHistory history =
        _accessor.getProperty(_accessor.keyBuilder().controllerLeaderHistory());
    Assert.assertNull(history);

    // Mark both live instances to be frozen, then entering freeze mode is complete
    for (int i = 0; i < 2; i++) {
      LiveInstance liveInstance = liveInstances.get(i);
      liveInstance.setStatus(LiveInstance.LiveInstanceStatus.FROZEN);
      PropertyKey liveInstanceKey =
          _accessor.keyBuilder().liveInstance(liveInstance.getInstanceName());
      _accessor.updateProperty(liveInstanceKey, liveInstance);
    }
    // Require cache refresh
    cache.notifyDataChange(HelixConstants.ChangeType.LIVE_INSTANCE);
    runPipeline(event, dataRefresh, false);
    managementModeStage.process(event);

    // Freeze mode is complete
    clusterStatus = _accessor.getProperty(_accessor.keyBuilder().clusterStatus());
    Assert.assertEquals(clusterStatus.getManagementMode(), ClusterManagementMode.Type.CLUSTER_FREEZE);
    Assert.assertEquals(clusterStatus.getManagementModeStatus(),
        ClusterManagementMode.Status.COMPLETED);

    // Management history is recorded
    history = _accessor.getProperty(_accessor.keyBuilder().controllerLeaderHistory());
    Assert.assertEquals(history.getManagementModeHistory().size(), 1);
    String lastHistory = history.getManagementModeHistory().get(0);
    Assert.assertTrue(lastHistory.contains("MODE=" + ClusterManagementMode.Type.CLUSTER_FREEZE));
    Assert.assertTrue(lastHistory.contains("STATUS=" + ClusterManagementMode.Status.COMPLETED));

    // No duplicate management mode history entries
    managementModeStage.process(event);
    history = _accessor.getProperty(_accessor.keyBuilder().controllerLeaderHistory());
    Assert.assertEquals(history.getManagementModeHistory().size(), 1);

    // Unfreeze cluster
    request = ClusterManagementModeRequest.newBuilder()
        .withClusterName(_clusterName)
        .withMode(ClusterManagementMode.Type.NORMAL)
        .withReason("test")
        .build();
    _gSetupTool.getClusterManagementTool().setClusterManagementMode(request);
    runPipeline(event, dataRefresh, false);
    managementModeStage.process(event);
    clusterStatus = _accessor.getProperty(_accessor.keyBuilder().clusterStatus());

    Assert.assertEquals(clusterStatus.getManagementMode(), ClusterManagementMode.Type.NORMAL);
    // In progress because a live instance is still frozen
    Assert.assertEquals(clusterStatus.getManagementModeStatus(),
        ClusterManagementMode.Status.IN_PROGRESS);

    // remove froze status to mark the live instances to be normal status
    for (int i = 0; i < 2; i++) {
      LiveInstance liveInstance = liveInstances.get(i);
      PropertyKey liveInstanceKey =
          _accessor.keyBuilder().liveInstance(liveInstance.getInstanceName());
      liveInstance.getRecord().getSimpleFields()
          .remove(LiveInstance.LiveInstanceProperty.STATUS.name());
      _accessor.setProperty(liveInstanceKey, liveInstance);
    }
    // Require cache refresh
    cache.notifyDataChange(HelixConstants.ChangeType.LIVE_INSTANCE);
    runPipeline(event, dataRefresh, false);
    try {
      managementModeStage.process(event);
    } catch (HelixException expected) {
      // It's expected because controller does not set for cluster.
      Assert.assertTrue(expected.getMessage()
          .startsWith("Failed to switch management mode pipeline, enabled=false"));
    }
    clusterStatus = _accessor.getProperty(_accessor.keyBuilder().clusterStatus());

    // Fully existed frozen mode
    Assert.assertEquals(clusterStatus.getManagementMode(), ClusterManagementMode.Type.NORMAL);
    Assert.assertEquals(clusterStatus.getManagementModeStatus(),
        ClusterManagementMode.Status.COMPLETED);
  }
}
