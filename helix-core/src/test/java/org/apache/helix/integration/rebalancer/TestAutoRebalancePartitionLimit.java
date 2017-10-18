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
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.ZkVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAutoRebalancePartitionLimit extends ZkStandAloneCMTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestAutoRebalancePartitionLimit.class
      .getName());

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(ZK_ADDR);

    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);

    _setupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, 100, "OnlineOffline",
        RebalanceMode.FULL_AUTO + "", 0, 25);
    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 1);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    HelixManager manager = _controller; // _startCMResultMap.get(controllerName)._manager;
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    // start dummy participants
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
      Thread.sleep(2000);
      boolean result =
          ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient,
              CLUSTER_NAME, TEST_DB));
      Assert.assertTrue(result);
      ExternalView ev =
          manager.getHelixDataAccessor().getProperty(accessor.keyBuilder().externalView(TEST_DB));
      System.out.println(ev.getPartitionSet().size());
      if (i < 3) {
        Assert.assertEquals(ev.getPartitionSet().size(), 25 * (i + 1));
      } else {
        Assert.assertEquals(ev.getPartitionSet().size(), 100);
      }
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient,
            CLUSTER_NAME, TEST_DB));

    Assert.assertTrue(result);
  }

  @Test()
  public void testAutoRebalanceWithMaxPartitionPerNode() throws Exception {
    HelixManager manager = _controller;
    // kill 1 node
    _participants[0].syncStop();

    // verifyBalanceExternalView();
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient,
            CLUSTER_NAME, TEST_DB));
    Assert.assertTrue(result);
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    ExternalView ev =
        manager.getHelixDataAccessor().getProperty(accessor.keyBuilder().externalView(TEST_DB));
    Assert.assertEquals(ev.getPartitionSet().size(), 100);

    _participants[1].syncStop();

    // verifyBalanceExternalView();
    result =
        ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient,
            CLUSTER_NAME, TEST_DB));
    Assert.assertTrue(result);
    ev = manager.getHelixDataAccessor().getProperty(accessor.keyBuilder().externalView(TEST_DB));
    Assert.assertEquals(ev.getPartitionSet().size(), 75);

    // add 2 nodes
    for (int i = 0; i < 2; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (1000 + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);

      String newInstanceName = storageNodeName.replace(':', '_');
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, newInstanceName);
      participant.syncStart();
    }

    Thread.sleep(1000);
    result =
        ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient,
            CLUSTER_NAME, TEST_DB));
    Assert.assertTrue(result);
  }

  static boolean verifyBalanceExternalView(ZNRecord externalView, int partitionCount,
      String masterState, int replica, int instances, int maxPerInstance) {
    Map<String, Integer> masterPartitionsCountMap = new HashMap<String, Integer>();
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
          || instancePartitionCount == perInstancePartition + 1 || instancePartitionCount == maxPerInstance)) {
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
    if (partitionCount != totalCount) {
      return false;
    }
    return true;

  }

  public static class ExternalViewBalancedVerifier implements ZkVerifier {
    String _clusterName;
    String _resourceName;

    public ExternalViewBalancedVerifier(ZkClient client, String clusterName, String resourceName) {
      _clusterName = clusterName;
      _resourceName = resourceName;
    }

    @Override
    public boolean verify() {
      HelixDataAccessor accessor =
          new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
      Builder keyBuilder = accessor.keyBuilder();
      int numberOfPartitions =
          accessor.getProperty(keyBuilder.idealStates(_resourceName)).getRecord().getListFields()
              .size();
      ClusterDataCache cache = new ClusterDataCache();
      cache.refresh(accessor);
      String masterValue =
          cache.getStateModelDef(cache.getIdealState(_resourceName).getStateModelDefRef())
              .getStatesPriorityList().get(0);
      int replicas = Integer.parseInt(cache.getIdealState(_resourceName).getReplicas());
      return verifyBalanceExternalView(accessor.getProperty(keyBuilder.externalView(_resourceName))
          .getRecord(), numberOfPartitions, masterValue, replicas, cache.getLiveInstances().size(),
          cache.getIdealState(_resourceName).getMaxPartitionsPerInstance());
    }

    @Override
    public ZkClient getZkClient() {
      return _gZkClient;
    }

    @Override
    public String getClusterName() {
      return _clusterName;
    }
  }
}
