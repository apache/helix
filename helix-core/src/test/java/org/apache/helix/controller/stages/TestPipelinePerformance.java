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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestPipelinePerformance extends ZkTestBase {

  private String _clusterName;
  private HelixManager _manager;
  private ExecutorService _asyncTaskThreadPool;
  private ResourceControllerDataProvider _clusterData;

  @BeforeTest
  private void setUpTest() {
    _clusterName = String.format("CLUSTER_%s", getShortClassName());
    HelixDataAccessor accessor = new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    accessor.setProperty(accessor.keyBuilder().clusterConfig(), new ClusterConfig(_clusterName));

    _asyncTaskThreadPool = Executors.newSingleThreadExecutor();
    _manager = new DummyClusterManager(_clusterName, accessor, Long.toHexString(_gZkClient.getSessionId()));
    _clusterData = new ResourceControllerDataProvider();
    _clusterData.setAsyncTasksThreadPool(_asyncTaskThreadPool);

  }

  @AfterTest
  private void windDownTest() {
    deleteLiveInstances(_clusterName);
    deleteCluster(_clusterName);
    _asyncTaskThreadPool.shutdown();
  }

  @Test
  public void testWagedInstanceCapacityCalculationPerformance() throws Exception {
    ClusterEvent event = new ClusterEvent(ClusterEventType.CurrentStateChange);
    event.addAttribute(AttributeName.helixmanager.name(), _manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), _clusterData);

    String[] resourceGroups = new String[] { "testResource_dup" };
    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    setupIdealState(_clusterName, new int[] { 0 }, resourceGroups, 1, 1);
    List<LiveInstance> liveInstances = setupLiveInstances(_clusterName, new int[] { 0 });
    setupStateModel(_clusterName);

    // cluster data cache refresh pipeline
    Pipeline dataRefreshPipeline = new Pipeline();
    dataRefreshPipeline.addStage(new ReadClusterDataStage());

    // computation pipeline
    Pipeline computationPipeline = new Pipeline();
    computationPipeline.addStage(new ResourceComputationStage());
    computationPipeline.addStage(new CurrentStateComputationStage());

    long roundOneStartTimestamp = System.currentTimeMillis();
    runPipeline(event, dataRefresh, false);
    runPipeline(event, rebalancePipeline, false);
    long roundOneDuration = System.currentTimeMillis() - roundOneStartTimestamp;
    System.out.println("-----> " + roundOneDuration);

    // round2: updates node0 currentState to SLAVE but keep the
    // message, make sure controller should not wait for the message to be deleted, but should
    // send out a S -> M message to node0

    long roundTwoStartTimestamp = System.currentTimeMillis();
    runPipeline(event, dataRefresh, false);
    runPipeline(event, rebalancePipeline, false);
    long roundTwoDuration = System.currentTimeMillis() - roundTwoStartTimestamp;
    System.out.println("-----> " + roundTwoDuration);
  }

  protected void setCurrentState(String clusterName, String instance, String resourceGroupName,
      String resourceKey, String sessionId, String state) {
    setCurrentState(clusterName, instance, resourceGroupName, resourceKey, sessionId, state, false);
  }

  private void setCurrentState(String clusterName, String instance, String resourceGroupName,
      String resourceKey, String sessionId, String state, boolean updateTimestamp) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    CurrentState curState = new CurrentState(resourceGroupName);
    curState.setState(resourceKey, state);
    curState.setSessionId(sessionId);
    curState.setStateModelDefRef("MasterSlave");
    if (updateTimestamp) {
      curState.setEndTime(resourceKey, System.currentTimeMillis());
    }
    accessor.setProperty(keyBuilder.currentState(instance, sessionId, resourceGroupName), curState);
  }

}
