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

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.ZkVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAutoRebalance extends ZkStandAloneCMTestBase {
  String db2 = TEST_DB + "2";
  String _tag = "SSDSSD";

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_zkclient.exists(namespace)) {
      _zkclient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(_zkclient);

    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);
    _setupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, _PARTITIONS, STATE_MODEL,
        RebalanceMode.FULL_AUTO + "");

    _setupTool.addResourceToCluster(CLUSTER_NAME, db2, _PARTITIONS, "OnlineOffline",
        RebalanceMode.FULL_AUTO + "");

    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = "localhost_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, _replica);

    for (int i = 0; i < 3; i++) {
      String storageNodeName = "localhost_" + (START_PORT + i);
      _setupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, storageNodeName, _tag);
    }

    _setupTool.rebalanceCluster(CLUSTER_NAME, db2, 1, "ucpx", _tag);

    // start dummy participants
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = "localhost_" + (START_PORT + i);
      MockParticipant participant =
          new MockParticipant(_zkaddr, CLUSTER_NAME, instanceName);
      participant.syncStart();
      _participants[i] = participant;

    }

    // start controller
    String controllerName = "controller_0";
    _controller = new MockController(_zkaddr, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_zkclient,
            CLUSTER_NAME, TEST_DB));

    Assert.assertTrue(result);

  }

  @Test()
  public void testDropResourceAutoRebalance() throws Exception {
    // add a resource to be dropped
    _setupTool.addResourceToCluster(CLUSTER_NAME, "MyDB", _PARTITIONS, "OnlineOffline",
        RebalanceMode.FULL_AUTO + "");

    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 1);

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_zkclient,
            CLUSTER_NAME, "MyDB"));
    Assert.assertTrue(result);

    String command = "-zkSvr " + _zkaddr + " -dropResource " + CLUSTER_NAME + " " + "MyDB";
    ClusterSetup.processCommandLineArgs(command.split(" "));

    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView", 30 * 1000, CLUSTER_NAME, "MyDB",
        TestHelper.<String> setOf("localhost_12918", "localhost_12919", "localhost_12920",
            "localhost_12921", "localhost_12922"), _zkaddr);

    // add a resource to be dropped
    _setupTool.addResourceToCluster(CLUSTER_NAME, "MyDB2", _PARTITIONS, "MasterSlave",
        RebalanceMode.FULL_AUTO + "");

    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB2", 3);

    result =
        ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_zkclient,
            CLUSTER_NAME, "MyDB2"));
    Assert.assertTrue(result);

    command = "-zkSvr " + _zkaddr + " -dropResource " + CLUSTER_NAME + " " + "MyDB2";
    ClusterSetup.processCommandLineArgs(command.split(" "));

    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView", 30 * 1000, CLUSTER_NAME, "MyDB2",
        TestHelper.<String> setOf("localhost_12918", "localhost_12919", "localhost_12920",
            "localhost_12921", "localhost_12922"), _zkaddr);
  }

  @Test()
  public void testAutoRebalance() throws Exception {
    // kill 1 node
    _participants[0].syncStop();

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_zkclient,
            CLUSTER_NAME, TEST_DB));
    Assert.assertTrue(result);

    // add 2 nodes
    for (int i = 0; i < 2; i++) {
      String storageNodeName = "localhost_" + (1000 + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);

      MockParticipant participant =
          new MockParticipant(_zkaddr, CLUSTER_NAME, storageNodeName.replace(':', '_'));
      participant.syncStart();
    }
    Thread.sleep(5000);
    result =
        ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_zkclient,
            CLUSTER_NAME, TEST_DB));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_zkclient,
            CLUSTER_NAME, db2));
    Assert.assertTrue(result);
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<ZNRecord>(_zkclient));
    Builder keyBuilder = accessor.keyBuilder();
    ExternalView ev = accessor.getProperty(keyBuilder.externalView(db2));
    Set<String> instancesSet = new HashSet<String>();
    for (String partitionName : ev.getRecord().getMapFields().keySet()) {
      Map<String, String> assignmentMap = ev.getRecord().getMapField(partitionName);
      for (String instance : assignmentMap.keySet()) {
        instancesSet.add(instance);
      }
    }
    Assert.assertEquals(instancesSet.size(), 2);
  }

  static boolean verifyBalanceExternalView(ZNRecord externalView, int partitionCount,
      String masterState, int replica, int instances) {
    if (externalView == null) {
      return false;
    }
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
      if (Math.abs(instancePartitionCount - perInstancePartition) > 1) {
        // System.out.println("instanceName: " + instanceName + ", instancePartitionCnt: "
        // + instancePartitionCount + ", perInstancePartition: " + perInstancePartition);
        return false;
      }
    }
    if (partitionCount != totalCount) {
      // System.out.println("partitionCnt: " + partitionCount + ", totalCnt: " + totalCount);
      return false;
    }
    return true;

  }

  public static class ExternalViewBalancedVerifier extends ZkVerifier {
    String _resourceName;

    public ExternalViewBalancedVerifier(ZkClient client, String clusterName, String resourceName) {
      super(clusterName, client);
      _resourceName = resourceName;
    }

    @Override
    public boolean verify() {
      HelixDataAccessor accessor =
          new ZKHelixDataAccessor(getClusterName(), _baseAccessor);
      Builder keyBuilder = accessor.keyBuilder();
      IdealState idealState = accessor.getProperty(keyBuilder.idealStates(_resourceName));
      if (idealState == null) {
        return false;
      }

      int numberOfPartitions = idealState.getRecord().getListFields().size();
      String stateModelDefName = idealState.getStateModelDefId().stringify();
      StateModelDefinition stateModelDef =
          accessor.getProperty(keyBuilder.stateModelDef(stateModelDefName));
      State masterValue = stateModelDef.getTypedStatesPriorityList().get(0);
      int replicas = Integer.parseInt(idealState.getReplicas());

      String instanceGroupTag = idealState.getInstanceGroupTag();

      int instances = 0;
      Map<String, LiveInstance> liveInstanceMap =
          accessor.getChildValuesMap(keyBuilder.liveInstances());
      Map<String, InstanceConfig> instanceConfigMap =
          accessor.getChildValuesMap(keyBuilder.instanceConfigs());
      for (String liveInstanceName : liveInstanceMap.keySet()) {
        if (instanceConfigMap.get(liveInstanceName).containsTag(instanceGroupTag)) {
          instances++;
        }
      }
      if (instances == 0) {
        instances = liveInstanceMap.size();
      }
      ExternalView ev = accessor.getProperty(keyBuilder.externalView(_resourceName));
      if (ev == null) {
        return false;
      }
      return verifyBalanceExternalView(ev.getRecord(), numberOfPartitions, masterValue.toString(),
          replicas, instances);
    }
  }
}
