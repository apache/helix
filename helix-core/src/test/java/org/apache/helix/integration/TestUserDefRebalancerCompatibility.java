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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("deprecation")
public class TestUserDefRebalancerCompatibility extends ZkStandAloneCMTestBase {
  String db2 = TEST_DB + "2";
  static boolean testRebalancerCreated = false;
  static boolean testRebalancerInvoked = false;

  public static class TestRebalancer implements Rebalancer {
    @Override
    public void init(HelixManager helixManager) {
      testRebalancerCreated = true;
    }

    /**
     * Very basic mapping that evenly assigns one replica of each partition to live nodes, each of
     * which is in the highest-priority state.
     */
    @Override
    public IdealState computeResourceMapping(String resourceName, IdealState currentIdealState,
        CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
      testRebalancerInvoked = true;
      for (String partition : currentIdealState.getPartitionSet()) {
        String instance = currentIdealState.getPreferenceList(partition).get(0);
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
        TestUserDefRebalancerCompatibility.TestRebalancer.class.getName());

    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, db2, 3);

    try {
      boolean result =
          ClusterStateVerifier
              .verifyByZkCallback(new TestCustomizedIdealStateRebalancer.ExternalViewBalancedVerifier(
                  _zkclient, CLUSTER_NAME, db2));
      Assert.assertTrue(result);
      Thread.sleep(1000);
      HelixDataAccessor accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
      Builder keyBuilder = accessor.keyBuilder();
      ExternalView ev = accessor.getProperty(keyBuilder.externalView(db2));
      Assert.assertEquals(ev.getPartitionSet().size(), 60);
      for (String partition : ev.getPartitionSet()) {
        Assert.assertEquals(ev.getStateMap(partition).size(), 1);
      }
      IdealState is = accessor.getProperty(keyBuilder.idealStates(db2));
      for (PartitionId partition : is.getPartitionIdSet()) {
        Assert.assertEquals(is.getPreferenceList(partition).size(), 3);
        Assert.assertEquals(is.getParticipantStateMap(partition).size(), 3);
      }
      Assert.assertTrue(testRebalancerCreated);
      Assert.assertTrue(testRebalancerInvoked);
    } finally {
      _setupTool.dropResourceFromCluster(CLUSTER_NAME, db2);
    }
  }
}
