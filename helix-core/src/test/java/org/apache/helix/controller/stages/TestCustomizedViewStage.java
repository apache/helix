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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.management.ObjectName;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.CustomizedState;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.CustomizedViewMonitor;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCustomizedViewStage extends ZkTestBase {
  private final String RESOURCE_NAME = "TestDB";
  private final String PARTITION_NAME = "TestDB_0";
  private final String CUSTOMIZED_STATE_NAME = "customizedState1";
  private final String INSTANCE_NAME = "localhost_1";

  @Test
  public void testCachedCustomizedViews() throws Exception {
    String clusterName = "CLUSTER_" + TestHelper.getTestMethodName();

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    HelixManager manager = new DummyClusterManager(clusterName, accessor);

    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    setupIdealState(clusterName, new int[]{0, 1}, new String[]{"TestDB"}, 1, 2);
    setupLiveInstances(clusterName, new int[]{0, 1});
    setupStateModel(clusterName);

    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(clusterName);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    CustomizedStateConfig config = new CustomizedStateConfig();
    List<String> aggregationEnabledTypes = new ArrayList<>();
    aggregationEnabledTypes.add(CUSTOMIZED_STATE_NAME);
    config.setAggregationEnabledTypes(aggregationEnabledTypes);

    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.customizedStateConfig(), config);

    CustomizedState customizedState = new CustomizedState(RESOURCE_NAME);
    customizedState.setState(PARTITION_NAME, "STARTED");
    accessor
        .setProperty(keyBuilder.customizedState(INSTANCE_NAME, "customizedState1", RESOURCE_NAME),
            customizedState);

    CustomizedViewAggregationStage customizedViewComputeStage =
        new CustomizedViewAggregationStage();
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());
    runPipeline(event, dataRefresh, false);
    runStage(event, new ResourceComputationStage());
    runStage(event, new CustomizedStateComputationStage());
    runStage(event, customizedViewComputeStage);
    Assert.assertEquals(cache.getCustomizedViewCacheMap().size(),
        accessor.getChildNames(accessor.keyBuilder().customizedViews()).size());

    // Assure there is no customized view got updated when running the stage again
    List<CustomizedView> oldCustomizedViews =
        accessor.getChildValues(accessor.keyBuilder().customizedViews(), true);
    runStage(event, customizedViewComputeStage);
    List<CustomizedView> newCustomizedViews =
        accessor.getChildValues(accessor.keyBuilder().customizedViews(), true);
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

  @Test
  public void testLatencyMetricReporting() throws Exception {
    String clusterName = "CLUSTER_" + TestHelper.getTestMethodName();

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    HelixManager manager = new DummyClusterManager(clusterName, accessor);

    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    setupIdealState(clusterName, new int[]{0, 1}, new String[]{"TestDB"}, 1, 2);
    setupLiveInstances(clusterName, new int[]{0, 1});
    setupStateModel(clusterName);

    ClusterStatusMonitor clusterStatusMonitor = new ClusterStatusMonitor(clusterName);
    ClusterEvent event = new ClusterEvent(clusterName, ClusterEventType.Unknown);
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(clusterName);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    cache.setAsyncTasksThreadPool(executor);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    event.addAttribute(AttributeName.clusterStatusMonitor.name(), clusterStatusMonitor);

    CustomizedStateConfig config = new CustomizedStateConfig();
    List<String> aggregationEnabledTypes = new ArrayList<>();
    aggregationEnabledTypes.add(CUSTOMIZED_STATE_NAME);
    config.setAggregationEnabledTypes(aggregationEnabledTypes);

    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.customizedStateConfig(), config);

    CustomizedState customizedState = new CustomizedState(RESOURCE_NAME);
    customizedState.setState(PARTITION_NAME, "STATE");
    customizedState.setStartTime(PARTITION_NAME, 1);
    accessor.setProperty(
        keyBuilder.customizedState(INSTANCE_NAME, CUSTOMIZED_STATE_NAME, RESOURCE_NAME),
        customizedState);

    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());
    runPipeline(event, dataRefresh, false);
    runStage(event, new ResourceComputationStage());
    runStage(event, new CustomizedStateComputationStage());
    runStage(event, new CustomizedViewAggregationStage());

    ObjectName objectName = new ObjectName(String
        .format("%s:%s=%s,%s=%s", MonitorDomainNames.AggregatedView.name(), "Type",
            "CustomizedView", "Cluster", clusterName));
    Field customizedViewMonitor =
        ClusterStatusMonitor.class.getDeclaredField("_customizedViewMonitor");
    Assert.assertNotNull(customizedViewMonitor);

    boolean hasLatencyReported = TestHelper.verify(() -> (long) _server.getAttribute(objectName,
        CustomizedViewMonitor.UPDATE_TO_AGGREGATION_LATENCY_GAUGE + ".Max") != 0,
        TestHelper.WAIT_DURATION);
    Assert.assertTrue(hasLatencyReported);

    deleteLiveInstances(clusterName);
    deleteCluster(clusterName);
    executor.shutdownNow();
  }

  @Test
  public void testLatencyCalculationWithEmptyTimestamp() throws Exception {
    String clusterName = "CLUSTER_" + TestHelper.getTestMethodName();

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    HelixManager manager = new DummyClusterManager(clusterName, accessor);

    setupIdealState(clusterName, new int[]{0, 1}, new String[]{"TestDB"}, 1, 2);
    setupLiveInstances(clusterName, new int[]{0, 1});
    setupStateModel(clusterName);

    ClusterStatusMonitor clusterStatusMonitor = new ClusterStatusMonitor(clusterName);
    ClusterEvent event = new ClusterEvent(clusterName, ClusterEventType.Unknown);
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(clusterName);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    cache.setAsyncTasksThreadPool(executor);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    event.addAttribute(AttributeName.clusterStatusMonitor.name(), clusterStatusMonitor);

    CustomizedStateConfig config = new CustomizedStateConfig();
    List<String> aggregationEnabledTypes = new ArrayList<>();
    aggregationEnabledTypes.add(CUSTOMIZED_STATE_NAME);
    config.setAggregationEnabledTypes(aggregationEnabledTypes);

    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.customizedStateConfig(), config);

    CustomizedState customizedState = new CustomizedState(RESOURCE_NAME);
    customizedState.setState(PARTITION_NAME, "STATE");
    accessor.setProperty(
        keyBuilder.customizedState(INSTANCE_NAME, CUSTOMIZED_STATE_NAME, RESOURCE_NAME),
        customizedState);

    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());
    runPipeline(event, dataRefresh, false);
    runStage(event, new ResourceComputationStage());
    runStage(event, new CustomizedStateComputationStage());
    runStage(event, new CustomizedViewAggregationStage());

    ObjectName objectName = new ObjectName(String
        .format("%s:%s=%s,%s=%s", MonitorDomainNames.AggregatedView.name(), "Type",
            "CustomizedView", "Cluster", clusterName));
    Field customizedViewMonitor =
        ClusterStatusMonitor.class.getDeclaredField("_customizedViewMonitor");
    Assert.assertNotNull(customizedViewMonitor);

    boolean hasLatencyReported = TestHelper.verify(() -> (long) _server.getAttribute(objectName,
        CustomizedViewMonitor.UPDATE_TO_AGGREGATION_LATENCY_GAUGE + ".Max") != 0,
        TestHelper.WAIT_DURATION);
    Assert.assertFalse(hasLatencyReported);

    deleteLiveInstances(clusterName);
    deleteCluster(clusterName);
    executor.shutdownNow();
  }
}
