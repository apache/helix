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

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.CustomizedState;
import org.apache.helix.model.CustomizedStateAggregationConfig;
import org.apache.helix.model.CustomizedView;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCustomizedViewStage extends ZkUnitTestBase {
  private final String RESOURCE_NAME = "testResourceName";
  private final String PARTITION_NAME = "testResourceName_0";
  private final String CUSTOMIZED_STATE_NAME = "customizedState1";
  private final String INSTANCE_NAME = "localhost_1";

  @Test
  public void testCachedCustomizedViews() throws Exception {
    String clusterName = "CLUSTER_" + TestHelper.getTestMethodName();

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    HelixManager manager = new DummyClusterManager(clusterName, accessor);

    setupLiveInstances(clusterName, new int[] {0, 1});
    setupStateModel(clusterName);

    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(clusterName);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    CustomizedStateAggregationConfig config = new CustomizedStateAggregationConfig();
    List<String> aggregationEnabledTypes = new ArrayList<>();
    aggregationEnabledTypes.add(CUSTOMIZED_STATE_NAME);
    config.setAggregationEnabledTypes(aggregationEnabledTypes);

    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.customizedStateAggregationConfig(), config);

    CustomizedState customizedState = new CustomizedState(RESOURCE_NAME);
    customizedState.setState(PARTITION_NAME, "STARTED");
    accessor.setProperty(
        keyBuilder.customizedState(INSTANCE_NAME, "customizedState1", RESOURCE_NAME),
        customizedState);

    CustomizedViewComputeStage customizedViewComputeStage = new CustomizedViewComputeStage();
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());
    runPipeline(event, dataRefresh);
    runStage(event, new ResourceComputationStage());
    runStage(event, new CustomizedStateComputationStage());
    runStage(event, customizedViewComputeStage);
    Assert.assertEquals(cache.getCustomizedViewCacheMap().values(),
        accessor.getChildValues(accessor.keyBuilder().customizedViews()));

    // Assure there is no customized view got updated
    List<CustomizedView> oldCustomizedViews =
        accessor.getChildValues(accessor.keyBuilder().customizedViews());
    runStage(event, customizedViewComputeStage);
    List<CustomizedView> newCustomizedViews =
        accessor.getChildValues(accessor.keyBuilder().customizedViews());
    Assert.assertEquals(oldCustomizedViews, newCustomizedViews);
    for (int i = 0; i < oldCustomizedViews.size(); i++) {
      Assert.assertEquals(oldCustomizedViews.get(i).getStat().getVersion(),
          newCustomizedViews.get(i).getStat().getVersion());
    }

    if (manager.isConnected()) {
      manager.disconnect(); // For DummyClusterManager, this is not necessary
    }
    deleteLiveInstances(clusterName);
    deleteCluster(clusterName);
  }
}
