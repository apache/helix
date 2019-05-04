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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestExternalViewStage extends ZkUnitTestBase {
  @Test
  public void testCachedExternalViews() throws Exception {
    String clusterName = "CLUSTER_" + TestHelper.getTestMethodName();

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    HelixManager manager = new DummyClusterManager(clusterName, accessor);

    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    setupIdealState(clusterName, new int[] {
        0, 1
    }, new String[] {
        "TestDB"
    }, 1, 2);
    setupLiveInstances(clusterName, new int[] {
        0, 1
    });
    setupStateModel(clusterName);

    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(clusterName);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    ExternalViewComputeStage externalViewComputeStage = new ExternalViewComputeStage();
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());
    runPipeline(event, dataRefresh);
    runStage(event, new ResourceComputationStage());
    runStage(event, new CurrentStateComputationStage());
    runStage(event, externalViewComputeStage);
    Assert.assertEquals(cache.getExternalViews().values(),
        accessor.getChildValues(accessor.keyBuilder().externalViews()));

    // Assure there is no external got updated
    List<ExternalView> oldExternalViews =
        accessor.getChildValues(accessor.keyBuilder().externalViews());
    runStage(event, externalViewComputeStage);
    List<ExternalView> newExternalViews =
        accessor.getChildValues(accessor.keyBuilder().externalViews());
    Assert.assertEquals(oldExternalViews, newExternalViews);
    for (int i = 0; i < oldExternalViews.size(); i++) {
      Assert.assertEquals(oldExternalViews.get(i).getStat().getVersion(),
          newExternalViews.get(i).getStat().getVersion());
    }

    if (manager.isConnected()) {
      manager.disconnect(); // For DummyClusterManager, this is not necessary
    }
    deleteLiveInstances(clusterName);
    deleteCluster(clusterName);
  }
}
