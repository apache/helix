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

import com.beust.jcommander.internal.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterVerifiers.ClusterStateVerifier;
import org.apache.helix.tools.ClusterVerifiers.ClusterStateVerifier.ZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCustomizedIdealStateRebalancer extends ZkStandAloneCMTestBase {
  String db2 = TEST_DB + "2";
  static boolean testRebalancerCreated = false;
  static boolean testRebalancerInvoked = false;

  public static class TestRebalancer implements Rebalancer {

    @Override
    public void init(HelixManager manager) {
      testRebalancerCreated = true;
    }

    @Override
    public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
        CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
      testRebalancerInvoked = true;
      List<String> liveNodes = Lists.newArrayList(clusterData.getLiveInstances().keySet());
      int i = 0;
      for (String partition : currentIdealState.getPartitionSet()) {
        int index = i++ % liveNodes.size();
        String instance = liveNodes.get(index);
        currentIdealState.getPreferenceList(partition).clear();
        currentIdealState.getPreferenceList(partition).add(instance);

        currentIdealState.getInstanceStateMap(partition).clear();
        currentIdealState.getInstanceStateMap(partition).put(instance, "MASTER");
      }
      currentIdealState.setReplicas("1");
      return currentIdealState;
    }
  }

  @Test
  public void testCustomizedIdealStateRebalancer() throws InterruptedException {
    _setupTool.addResourceToCluster(CLUSTER_NAME, db2, 60, "MasterSlave");
    _setupTool.addResourceProperty(CLUSTER_NAME, db2,
        IdealStateProperty.REBALANCER_CLASS_NAME.toString(),
        TestCustomizedIdealStateRebalancer.TestRebalancer.class.getName());
    _setupTool.addResourceProperty(CLUSTER_NAME, db2, IdealStateProperty.REBALANCE_MODE.toString(),
        RebalanceMode.USER_DEFINED.toString());

    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, db2, 3);

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient,
            CLUSTER_NAME, db2));
    Assert.assertTrue(result);
    Thread.sleep(1000);
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();
    ExternalView ev = accessor.getProperty(keyBuilder.externalView(db2));
    Assert.assertEquals(ev.getPartitionSet().size(), 60);
    for (String partition : ev.getPartitionSet()) {
      Assert.assertEquals(ev.getStateMap(partition).size(), 1);
    }
    IdealState is = accessor.getProperty(keyBuilder.idealStates(db2));
    for (String partition : is.getPartitionSet()) {
      Assert.assertEquals(is.getPreferenceList(partition).size(), 0);
      Assert.assertEquals(is.getInstanceStateMap(partition).size(), 0);
    }
    Assert.assertTrue(testRebalancerCreated);
    Assert.assertTrue(testRebalancerInvoked);
  }

  public static class ExternalViewBalancedVerifier implements ZkVerifier {
    ZkClient _client;
    String _clusterName;
    String _resourceName;

    public ExternalViewBalancedVerifier(ZkClient client, String clusterName, String resourceName) {
      _client = client;
      _clusterName = clusterName;
      _resourceName = resourceName;
    }

    @Override
    public boolean verify() {
      try {
        HelixDataAccessor accessor =
            new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor<ZNRecord>(_client));
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
        return verifyBalanceExternalView(
            accessor.getProperty(keyBuilder.externalView(_resourceName)).getRecord(),
            numberOfPartitions, masterValue, replicas, instances);
      } catch (Exception e) {
        return false;
      }
    }

    @Override
    public ZkClient getZkClient() {
      return _client;
    }

    @Override
    public String getClusterName() {
      return _clusterName;
    }

  }

  static boolean verifyBalanceExternalView(ZNRecord externalView, int partitionCount,
      String masterState, int replica, int instances) {
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
      if (!(instancePartitionCount == perInstancePartition || instancePartitionCount == perInstancePartition + 1)) {
        return false;
      }
      if (instancePartitionCount == perInstancePartition + 1) {
        if (partitionCount % instances == 0) {
          return false;
        }
      }
    }
    if (partitionCount != totalCount) {
      return false;
    }
    return true;

  }
}
