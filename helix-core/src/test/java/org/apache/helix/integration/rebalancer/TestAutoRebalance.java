package org.apache.helix.integration.rebalancer;

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

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.ZkVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAutoRebalance extends ZkStandAloneCMTestBase {
  private String db2 = TEST_DB + "2";
  private String _tag = "SSDSSD";
  private Set<MockParticipantManager> _extraParticipants;

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    // Cache references to mock participants for teardown
    _extraParticipants = new HashSet<>();

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, _PARTITIONS, STATE_MODEL,
        RebalanceMode.FULL_AUTO + "");

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, db2, _PARTITIONS, "OnlineOffline",
        RebalanceMode.FULL_AUTO + "");

    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, _replica);

    for (int i = 0; i < 3; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, storageNodeName, _tag);
    }

    _gSetupTool.rebalanceCluster(CLUSTER_NAME, db2, 1, "ucpx", _tag);

    // start dummy participants
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      participant.syncStart();
      _participants[i] = participant;
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    boolean result = ClusterStateVerifier
        .verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient, CLUSTER_NAME, TEST_DB));

    Assert.assertTrue(result);

  }

  @Override
  @AfterClass
  public void afterClass() throws Exception {
    for (MockParticipantManager participantManager : _extraParticipants) {
      participantManager.syncStop();
    }
    super.afterClass();
  }

  @Test()
  public void testDropResourceAutoRebalance() throws Exception {
    // add a resource to be dropped
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "MyDB", _PARTITIONS, "OnlineOffline",
        RebalanceMode.FULL_AUTO + "");

    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 1);

    boolean result = ClusterStateVerifier
        .verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient, CLUSTER_NAME, "MyDB"));
    Assert.assertTrue(result);

    String command = "-zkSvr " + ZK_ADDR + " -dropResource " + CLUSTER_NAME + " " + "MyDB";
    ClusterSetup.processCommandLineArgs(command.split(" "));

    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView", 30 * 1000, CLUSTER_NAME, "MyDB",
        TestHelper.setOf("localhost_12918", "localhost_12919", "localhost_12920", "localhost_12921",
            "localhost_12922"),
        ZK_ADDR);

    // add a resource to be dropped
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "MyDB2", _PARTITIONS, "MasterSlave",
        RebalanceMode.FULL_AUTO + "");

    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB2", 1);

    result = ClusterStateVerifier
        .verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient, CLUSTER_NAME, "MyDB2"));
    Assert.assertTrue(result);

    command = "-zkSvr " + ZK_ADDR + " -dropResource " + CLUSTER_NAME + " " + "MyDB2";
    ClusterSetup.processCommandLineArgs(command.split(" "));

    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView", 30 * 1000, CLUSTER_NAME, "MyDB2",
        TestHelper.setOf("localhost_12918", "localhost_12919", "localhost_12920", "localhost_12921",
            "localhost_12922"),
        ZK_ADDR);
  }

  @Test()
  public void testAutoRebalance() throws Exception {
    // kill 1 node
    _participants[0].syncStop();

    boolean result = ClusterStateVerifier
        .verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient, CLUSTER_NAME, TEST_DB));
    Assert.assertTrue(result);

    // add 2 nodes
    for (int i = 0; i < 2; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (1000 + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);

      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName.replace(':', '_'));
      _extraParticipants.add(participant);
      participant.syncStart();
    }
    Thread.sleep(100);
    result = ClusterStateVerifier.verifyByPolling(
        new ExternalViewBalancedVerifier(_gZkClient, CLUSTER_NAME, TEST_DB), 10000, 100);
    Assert.assertTrue(result);

    result = ClusterStateVerifier
        .verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient, CLUSTER_NAME, db2));
    Assert.assertTrue(result);
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();
    ExternalView ev = accessor.getProperty(keyBuilder.externalView(db2));
    Set<String> instancesSet = new HashSet<>();
    for (String partitionName : ev.getRecord().getMapFields().keySet()) {
      Map<String, String> assignmentMap = ev.getRecord().getMapField(partitionName);
      instancesSet.addAll(assignmentMap.keySet());
    }
    Assert.assertEquals(instancesSet.size(), 2);
  }

  static boolean verifyBalanceExternalView(ZNRecord externalView, int partitionCount,
      String masterState, int replica, int instances) {
    if (externalView == null) {
      return false;
    }
    Map<String, Integer> masterPartitionsCountMap = new HashMap<>();
    for (String partitionName : externalView.getMapFields().keySet()) {
      Map<String, String> assignmentMap = externalView.getMapField(partitionName);
      // Assert.assertTrue(assignmentMap.size() >= replica);
      for (String instance : assignmentMap.keySet()) {
        if (assignmentMap.get(instance).equals(masterState)) {
          if (!masterPartitionsCountMap.containsKey(instance)) {
            masterPartitionsCountMap.put(instance, 0);
          }
          masterPartitionsCountMap.put(instance, masterPartitionsCountMap.get(instance) + 1);
        }
      }
    }

    int perInstancePartition = partitionCount / instances;

    int totalCount = 0;
    for (String instanceName : masterPartitionsCountMap.keySet()) {
      int instancePartitionCount = masterPartitionsCountMap.get(instanceName);
      totalCount += instancePartitionCount;
      if (!(instancePartitionCount == perInstancePartition
          || instancePartitionCount == perInstancePartition + 1)) {
        return false;
      }
      if (instancePartitionCount == perInstancePartition + 1) {
        if (partitionCount % instances == 0) {
          return false;
        }
      }
    }
    return partitionCount == totalCount;
  }

  public static class ExternalViewBalancedVerifier implements ZkVerifier {
    String _clusterName;
    String _resourceName;
    HelixZkClient _client;

    public ExternalViewBalancedVerifier(HelixZkClient client, String clusterName,
        String resourceName) {
      _clusterName = clusterName;
      _resourceName = resourceName;
      _client = client;
    }

    @Override
    public boolean verify() {
      HelixDataAccessor accessor =
          new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor<>(_gZkClient));
      Builder keyBuilder = accessor.keyBuilder();
      int numberOfPartitions;
      try {
        numberOfPartitions = accessor.getProperty(keyBuilder.idealStates(_resourceName)).getRecord()
            .getListFields().size();
      } catch (Exception e) {
        return false;
      }
      ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
      cache.refresh(accessor);

      IdealState idealState = cache.getIdealState(_resourceName);
      if (idealState == null) {
        return false;
      }
      String masterValue =
          cache.getStateModelDef(idealState.getStateModelDefRef()).getStatesPriorityList().get(0);
      int replicas = Integer.parseInt(cache.getIdealState(_resourceName).getReplicas());
      String instanceGroupTag = cache.getIdealState(_resourceName).getInstanceGroupTag();
      int instances = 0;
      for (String liveInstanceName : cache.getLiveInstances().keySet()) {
        if (cache.getInstanceConfigMap().get(liveInstanceName).containsTag(instanceGroupTag)) {
          instances++;
        }
      }
      if (instances == 0) {
        instances = cache.getLiveInstances().size();
      }
      ExternalView ev = accessor.getProperty(keyBuilder.externalView(_resourceName));
      if (ev == null) {
        return false;
      }
      return verifyBalanceExternalView(ev.getRecord(), numberOfPartitions, masterValue, replicas,
          instances);
    }

    @Override
    public ZkClient getZkClient() {
      return (ZkClient) _client;
    }

    @Override
    public String getClusterName() {
      return _clusterName;
    }
  }
}
