package org.apache.helix.integration.rebalancer.WagedRebalancer;

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

import org.apache.helix.integration.rebalancer.TestMixedModeAutoRebalance;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;

public class TestMixedModeWagedRebalance extends TestMixedModeAutoRebalance {
  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  @DataProvider(name = "stateModels")
  public static Object[][] stateModels() {
    return new Object[][] { { BuiltInStateModelDefinitions.MasterSlave.name(), true, null },
        { BuiltInStateModelDefinitions.OnlineOffline.name(), true, null },
        { BuiltInStateModelDefinitions.LeaderStandby.name(), true, null },
        { BuiltInStateModelDefinitions.MasterSlave.name(), false, null },
        { BuiltInStateModelDefinitions.OnlineOffline.name(), false, null },
        { BuiltInStateModelDefinitions.LeaderStandby.name(), false, null }
    };
  }

  protected ZkHelixClusterVerifier getClusterVerifier() {
    return new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
  }

  protected void createResource(String stateModel, int numPartition,
      int replica, boolean delayEnabled, String rebalanceStrategy) {
    if (delayEnabled) {
      setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, 200);
      createResourceWithWagedRebalance(CLUSTER_NAME, _db, stateModel, numPartition, replica,
          replica - 1);
    } else {
      createResourceWithWagedRebalance(CLUSTER_NAME, _db, stateModel, numPartition, replica, replica);
    }
  }

  @AfterMethod
  public void afterMethod() {
    super.afterMethod();
    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, -1);
  }
}
