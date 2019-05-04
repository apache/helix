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
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
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
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.ZkVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAutoRebalancePartitionLimit extends ZkStandAloneCMTestBase {

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, 100, "OnlineOffline",
        RebalanceMode.FULL_AUTO + "", 0, 25);
    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 1);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    HelixManager manager = _controller;
    HelixDataAccessor accessor = manager.getHelixDataAccessor();

    // start dummy participants
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
      Thread.sleep(100);
      boolean result = ClusterStateVerifier.verifyByPolling(
          new ExternalViewBalancedVerifier(_gZkClient, CLUSTER_NAME, TEST_DB), 10000, 100);
      Assert.assertTrue(result);
      ExternalView ev =
          manager.getHelixDataAccessor().getProperty(accessor.keyBuilder().externalView(TEST_DB));
      if (i < 3) {
        Assert.assertEquals(ev.getPartitionSet().size(), 25 * (i + 1));
      } else {
        Assert.assertEquals(ev.getPartitionSet().size(), 100);
      }
    }

    boolean result = ClusterStateVerifier
        .verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient, CLUSTER_NAME, TEST_DB));

    Assert.assertTrue(result);
  }

  @Test()
  public void testAutoRebalanceWithMaxPartitionPerNode() {
    HelixManager manager = _controller;
    // kill 1 node
    _participants[0].syncStop();

    boolean result = ClusterStateVerifier
        .verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient, CLUSTER_NAME, TEST_DB));
    Assert.assertTrue(result);
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    ExternalView ev =
        manager.getHelixDataAccessor().getProperty(accessor.keyBuilder().externalView(TEST_DB));
    Assert.assertEquals(ev.getPartitionSet().size(), 100);

    _participants[1].syncStop();

    result = ClusterStateVerifier
        .verifyByPolling(new ExternalViewBalancedVerifier(_gZkClient, CLUSTER_NAME, TEST_DB));
    Assert.assertTrue(result);
    ev = manager.getHelixDataAccessor().getProperty(accessor.keyBuilder().externalView(TEST_DB));
    Assert.assertEquals(ev.getPartitionSet().size(), 75);

    // add 2 nodes
    MockParticipantManager[] newParticipants = new MockParticipantManager[2];
    for (int i = 0; i < 2; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (1000 + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);

      String newInstanceName = storageNodeName.replace(':', '_');
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, newInstanceName);
      newParticipants[i] = participant;
      participant.syncStart();
    }

    Assert.assertTrue(ClusterStateVerifier.verifyByPolling(
        new ExternalViewBalancedVerifier(_gZkClient, CLUSTER_NAME, TEST_DB), 10000, 100));

    // Clean up the extra mock participants
    for (MockParticipantManager participant : newParticipants) {
      if (participant != null && participant.isConnected()) {
        participant.syncStop();
      }
    }
  }

  private static boolean verifyBalanceExternalView(ZNRecord externalView, int partitionCount,
      String masterState, int replica, int instances, int maxPerInstance) {
    Map<String, Integer> masterPartitionsCountMap = new HashMap<>();
    for (String partitionName : externalView.getMapFields().keySet()) {
      Map<String, String> assignmentMap = externalView.getMapField(partitionName);
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
          || instancePartitionCount == perInstancePartition + 1
          || instancePartitionCount == maxPerInstance)) {
        return false;
      }
      if (instancePartitionCount == maxPerInstance) {
        continue;
      }
      if (instancePartitionCount == perInstancePartition + 1) {
        if (partitionCount % instances == 0) {
          return false;
        }
      }
    }
    if (totalCount == maxPerInstance * instances) {
      return true;
    }
    return partitionCount == totalCount;
  }

  public static class ExternalViewBalancedVerifier implements ZkVerifier {
    String _clusterName;
    String _resourceName;
    HelixZkClient _client;

    ExternalViewBalancedVerifier(HelixZkClient client, String clusterName, String resourceName) {
      _clusterName = clusterName;
      _resourceName = resourceName;
      _client = client;
    }

    @Override
    public boolean verify() {
      HelixDataAccessor accessor =
          new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor<>(_gZkClient));
      Builder keyBuilder = accessor.keyBuilder();
      int numberOfPartitions = accessor.getProperty(keyBuilder.idealStates(_resourceName))
          .getRecord().getListFields().size();
      ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
      cache.refresh(accessor);
      String masterValue =
          cache.getStateModelDef(cache.getIdealState(_resourceName).getStateModelDefRef())
              .getStatesPriorityList().get(0);
      int replicas = Integer.parseInt(cache.getIdealState(_resourceName).getReplicas());
      return verifyBalanceExternalView(
          accessor.getProperty(keyBuilder.externalView(_resourceName)).getRecord(),
          numberOfPartitions, masterValue, replicas, cache.getLiveInstances().size(),
          cache.getIdealState(_resourceName).getMaxPartitionsPerInstance());
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
