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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.common.zkVerifiers.ExternalViewBalancedVerifier;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.ZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCustomizedIdealStateRebalancer extends ZkStandAloneCMTestBase {
  String db2 = TEST_DB + "2";
  static boolean testRebalancerCreated = false;
  static boolean testRebalancerInvoked = false;

  public static class TestRebalancer implements Rebalancer<ResourceControllerDataProvider> {

    @Override
    public void init(HelixManager manager) {
      testRebalancerCreated = true;
    }

    @Override
    public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
        CurrentStateOutput currentStateOutput, ResourceControllerDataProvider clusterData) {
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
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, db2, 60, "MasterSlave");
    _gSetupTool.addResourceProperty(CLUSTER_NAME, db2,
        IdealStateProperty.REBALANCER_CLASS_NAME.toString(),
        TestCustomizedIdealStateRebalancer.TestRebalancer.class.getName());
    _gSetupTool.addResourceProperty(CLUSTER_NAME, db2, IdealStateProperty.REBALANCE_MODE.toString(),
        RebalanceMode.USER_DEFINED.toString());

    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db2, 3);

    boolean result = ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_gZkClient,
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

}
