package org.apache.helix.integration;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ClusterLiveNodesVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestPartitionMovementThrottle extends ZkStandAloneCMTestBase {

  private ConfigAccessor _configAccessor;
  private Set<String> _dbs = new HashSet<>();

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // add dummy participants
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      participant.setTransition(new DelayedTransition());
      _participants[i] = participant;
    }

    _configAccessor = new ConfigAccessor(_gZkClient);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    setupThrottleConfig();

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  private void setupThrottleConfig() {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);

    StateTransitionThrottleConfig resourceLoadThrottle =
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.RESOURCE, 2);

    StateTransitionThrottleConfig instanceLoadThrottle =
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, 2);

    StateTransitionThrottleConfig clusterLoadThrottle =
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 100);

    StateTransitionThrottleConfig resourceRecoveryThrottle = new StateTransitionThrottleConfig(
        StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
        StateTransitionThrottleConfig.ThrottleScope.RESOURCE, 3);

    StateTransitionThrottleConfig clusterRecoveryThrottle = new StateTransitionThrottleConfig(
        StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
        StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 100);

    clusterConfig
        .setStateTransitionThrottleConfigs(Arrays.asList(resourceLoadThrottle, instanceLoadThrottle,
            clusterLoadThrottle, resourceRecoveryThrottle, clusterRecoveryThrottle));

    clusterConfig.setPersistIntermediateAssignment(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  @Test()
  public void testResourceThrottle() throws Exception {
    // start a few participants
    for (int i = 0; i < NODE_NR - 2; i++) {
      _participants[i].syncStart();
    }

    for (int i = 0; i < 5; i++) {
      String dbName = "TestDB-" + i;
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, dbName, 10, STATE_MODEL,
          RebalanceMode.FULL_AUTO + "");
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, dbName, _replica);
      _dbs.add(dbName);
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    DelayedTransition.setDelay(20);
    DelayedTransition.enableThrottleRecord();

    // add 2 nodes
    for (int i = NODE_NR - 2; i < NODE_NR; i++) {
      _participants[i].syncStart();
    }

    Thread.sleep(2000);

    for (String db : _dbs) {
      // After the fix in IntermediateCalcStage where downward load-balance is now allowed even if
      // there are recovery or error partitions present, maxPendingTransition below is adjusted from
      // 2 to 5 because BOTH recovery balance and load balance could happen in the same pipeline
      // iteration
      Assert.assertTrue(getMaxParallelTransitionCount(
          DelayedTransition.getResourcePatitionTransitionTimes(), db) <= 5);
    }
  }

  @Test
  public void testPartitionRecoveryRebalanceThrottle() throws InterruptedException {
    // start some participants
    for (int i = 0; i < NODE_NR - 2; i++) {
      _participants[i].syncStart();
    }
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, 10,
        STATE_MODEL, RebalanceMode.FULL_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, _replica);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Set throttling after states are stable. Otherwise it takes too long to reach stable state
    setSingleThrottlingConfig(StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE, 2);

    DelayedTransition.setDelay(20);
    DelayedTransition.enableThrottleRecord();

    // start another 2 nodes
    for (int i = NODE_NR - 2; i < NODE_NR; i++) {
      _participants[i].syncStart();
    }

    Thread.sleep(2000);

    for (int i = 0; i < NODE_NR; i++) {
      Assert.assertTrue(
          getMaxParallelTransitionCount(DelayedTransition.getInstancePatitionTransitionTimes(),
              _participants[i].getInstanceName()) <= 2);
    }
  }

  @Test
  public void testANYtypeThrottle() throws InterruptedException {
    // start some participants
    for (int i = 0; i < NODE_NR - 3; i++) {
      _participants[i].syncStart();
    }
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + "_ANY", 20,
        STATE_MODEL, RebalanceMode.FULL_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + "_ANY",
        _replica);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Set ANY type throttling after states are stable.
    setSingleThrottlingConfig(StateTransitionThrottleConfig.RebalanceType.ANY, 1);

    DelayedTransition.setDelay(20);
    DelayedTransition.enableThrottleRecord();

    // start another 3 nodes
    for (int i = NODE_NR - 3; i < NODE_NR; i++) {
      _participants[i].syncStart();
    }

    Thread.sleep(2000L);

    for (int i = 0; i < NODE_NR; i++) {
      Assert.assertTrue(
          getMaxParallelTransitionCount(DelayedTransition.getInstancePatitionTransitionTimes(),
              _participants[i].getInstanceName()) <= 1);
    }
  }

  @Test
  public void testThrottleOnlyClusterLevelAnyType() {
    // start some participants
    for (int i = 0; i < NODE_NR - 3; i++) {
      _participants[i].syncStart();
    }
    // Add resource: TestDB_ANY of 20 partitions
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + "_OnlyANY",
        20, STATE_MODEL, RebalanceMode.FULL_AUTO.name());
    // Act the rebalance process
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + "_OnlyANY",
        _replica);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // overwrite the cluster level throttle configuration
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    StateTransitionThrottleConfig anyTypeClusterThrottle =
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.ANY,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 1);
    clusterConfig.setStateTransitionThrottleConfigs(ImmutableList.of(anyTypeClusterThrottle));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    DelayedTransition.setDelay(20);
    DelayedTransition.enableThrottleRecord();

    List<MockParticipantManager> newNodes =
        Arrays.asList(_participants).subList(NODE_NR - 3, NODE_NR);
    newNodes.forEach(MockParticipantManager::syncStart);
    newNodes.forEach(node -> {
      try {
        Assert.assertTrue(TestHelper.verify(() -> getMaxParallelTransitionCount(
            DelayedTransition.getInstancePatitionTransitionTimes(), node.getInstanceName()) <= 1,
            1000 * 2));
      } catch (Exception e) {
        e.printStackTrace();
        assert false;
      }
    });

    ClusterLiveNodesVerifier liveNodesVerifier =
        new ClusterLiveNodesVerifier(_gZkClient, CLUSTER_NAME,
            Lists.transform(Arrays.asList(_participants), MockParticipantManager::getInstanceName));
    Assert.assertTrue(liveNodesVerifier.verifyByZkCallback(1000));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].syncStop();
    }
  }

  @AfterMethod
  public void cleanupTest() throws InterruptedException {
    for (String db : _dbs) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, db);
      Thread.sleep(20);
    }
    _dbs.clear();
    Thread.sleep(50);

    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<>(_gZkClient));
    for (int i = 0; i < _participants.length; i++) {
      _participants[i].syncStop();
      _participants[i] =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, _participants[i].getInstanceName());
    }
    try {
      Assert.assertTrue(TestHelper.verify(() -> dataAccessor.getChildNames(dataAccessor.keyBuilder().liveInstances()).isEmpty(), 1000));
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("There're live instances not cleaned up yet");
      assert false;
    }
    DelayedTransition.clearThrottleRecord();
  }

  @Test(dependsOnMethods = {
      "testResourceThrottle"
  })
  public void testResourceThrottleWithDelayRebalancer() {
    // start a few participants
    for (int i = 0; i < NODE_NR - 2; i++) {
      _participants[i].syncStart();
    }

    int partition = 10;
    int replica = 3;
    int minActiveReplica = 2;
    int delay = 100;

    for (int i = 0; i < 5; i++) {
      String dbName = "TestDB-" + i;
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, dbName);
      if (is != null) {
        System.err.println(dbName + "exists!");
        is.setReplicas(String.valueOf(replica));
        is.setMinActiveReplicas(minActiveReplica);
        is.setRebalanceDelay(delay);
        is.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
        _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, dbName, is);
      } else {
        createResourceWithDelayedRebalance(CLUSTER_NAME, dbName, STATE_MODEL, partition, replica,
            minActiveReplica, delay);
      }
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, dbName, _replica);
      _dbs.add(dbName);
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    DelayedTransition.setDelay(20);
    DelayedTransition.enableThrottleRecord();

    // add 2 nodes
    for (int i = NODE_NR - 2; i < NODE_NR; i++) {
      _participants[i].syncStart();
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    for (String db : _dbs) {
      int maxInParallel =
          getMaxParallelTransitionCount(DelayedTransition.getResourcePatitionTransitionTimes(), db);
      System.out.println("MaxInParallel: " + maxInParallel + " maxPendingTransition: " + 2);
      Assert.assertTrue(maxInParallel <= 2, "Throttle condition does not meet for " + db);
    }
  }

  private int getMaxParallelTransitionCount(
      Map<String, List<PartitionTransitionTime>> partitionTransitionTimesMap,
      String throttledItemName) {
    List<PartitionTransitionTime> pTimeList = partitionTransitionTimesMap.get(throttledItemName);

    Map<Long, List<PartitionTransitionTime>> startMap = new HashMap<>();
    Map<Long, List<PartitionTransitionTime>> endMap = new HashMap<>();
    List<Long> startEndPoints = new ArrayList<>();

    if (pTimeList == null) {
      System.out.println("no throttle result for :" + throttledItemName);
      return -1;
    }
    pTimeList.sort((o1, o2) -> (int) (o1.start - o2.start));

    for (PartitionTransitionTime interval : pTimeList) {
      if (!startMap.containsKey(interval.start)) {
        startMap.put(interval.start, new ArrayList<>());
      }
      startMap.get(interval.start).add(interval);

      if (!endMap.containsKey(interval.end)) {
        endMap.put(interval.end, new ArrayList<>());
      }
      endMap.get(interval.end).add(interval);
      startEndPoints.add(interval.start);
      startEndPoints.add(interval.end);
    }

    Collections.sort(startEndPoints);

    List<PartitionTransitionTime> temp = new ArrayList<>();

    int maxInParallel = 0;
    for (long point : startEndPoints) {
      if (startMap.containsKey(point)) {
        temp.addAll(startMap.get(point));
      }
      int curSize = size(temp);
      if (curSize > maxInParallel) {
        maxInParallel = curSize;
      }
      if (endMap.containsKey(point)) {
        temp.removeAll(endMap.get(point));
      }
    }

    System.out
        .println("Max number of ST in parallel: " + maxInParallel + " for " + throttledItemName);
    return maxInParallel;
  }

  private int size(List<PartitionTransitionTime> timeList) {
    Set<String> partitions = new HashSet<>();
    for (PartitionTransitionTime p : timeList) {
      partitions.add(p.partition);
    }
    return partitions.size();
  }

  private static class PartitionTransitionTime {
    String partition;
    long start;
    long end;

    PartitionTransitionTime(String partition, long start, long end) {
      this.partition = partition;
      this.start = start;
      this.end = end;
    }

    @Override
    public String toString() {
      return "[" + "partition='" + partition + '\'' + ", start=" + start + ", end=" + end + ']';
    }
  }

  private void setSingleThrottlingConfig(StateTransitionThrottleConfig.RebalanceType rebalanceType,
      int maxStateTransitions) {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    StateTransitionThrottleConfig anyTypeInstanceThrottle = new StateTransitionThrottleConfig(
        rebalanceType, StateTransitionThrottleConfig.ThrottleScope.INSTANCE, maxStateTransitions);
    List<StateTransitionThrottleConfig> currentThrottleConfigs =
        clusterConfig.getStateTransitionThrottleConfigs();
    currentThrottleConfigs.add(anyTypeInstanceThrottle);
    clusterConfig.setStateTransitionThrottleConfigs(currentThrottleConfigs);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  private static class DelayedTransition extends DelayedTransitionBase {
    private static Map<String, List<PartitionTransitionTime>> resourcePatitionTransitionTimes =
        new ConcurrentHashMap<>();
    private static Map<String, List<PartitionTransitionTime>> instancePatitionTransitionTimes =
        new ConcurrentHashMap<>();
    private static boolean _recordThrottle = false;

    static Map<String, List<PartitionTransitionTime>> getResourcePatitionTransitionTimes() {
      return resourcePatitionTransitionTimes;
    }

    static Map<String, List<PartitionTransitionTime>> getInstancePatitionTransitionTimes() {
      return instancePatitionTransitionTimes;
    }

    static void enableThrottleRecord() {
      _recordThrottle = true;
    }

    static void clearThrottleRecord() {
      resourcePatitionTransitionTimes.clear();
      instancePatitionTransitionTimes.clear();
    }

    @Override
    public void doTransition(Message message, NotificationContext context)
        throws InterruptedException {
      long start = System.currentTimeMillis();
      if (_delay > 0) {
        Thread.sleep(_delay);
      }
      long end = System.currentTimeMillis();
      if (_recordThrottle) {
        PartitionTransitionTime partitionTransitionTime =
            new PartitionTransitionTime(message.getPartitionName(), start, end);

        if (!resourcePatitionTransitionTimes.containsKey(message.getResourceName())) {
          resourcePatitionTransitionTimes.put(message.getResourceName(),
              Collections.synchronizedList(new ArrayList<>()));
        }
        resourcePatitionTransitionTimes.get(message.getResourceName()).add(partitionTransitionTime);

        if (!instancePatitionTransitionTimes.containsKey(message.getTgtName())) {
          instancePatitionTransitionTimes.put(message.getTgtName(),
              Collections.synchronizedList(new ArrayList<>()));
        }
        instancePatitionTransitionTimes.get(message.getTgtName()).add(partitionTransitionTime);
      }
    }
  }
}
