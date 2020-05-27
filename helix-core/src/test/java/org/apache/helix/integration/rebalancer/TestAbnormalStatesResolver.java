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

import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.api.rebalancer.constraint.AbnormalStateResolver;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.constraint.MockAbnormalStateResolver;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.MasterSlaveSMD;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAbnormalStatesResolver extends ZkStandAloneCMTestBase {
  @Test
  public void testConfigureResolver() {
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(CLUSTER_NAME);
    // Verify the initial setup.
    cache.refresh(_controller.getHelixDataAccessor());
    for (String stateModelDefName : cache.getStateModelDefMap().keySet()) {
      Assert.assertEquals(cache.getAbnormalStateResolver(stateModelDefName).getClass(),
          AbnormalStateResolver.DUMMY_STATE_RESOLVER.getClass());
    }

    // Update the resolver configuration for MasterSlave state model.
    ConfigAccessor configAccessor = new ConfigAccessor.Builder().setZkAddress(ZK_ADDR).build();
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setAbnormalStateResolverMap(
        ImmutableMap.of(MasterSlaveSMD.name, MockAbnormalStateResolver.class.getName()));
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    cache.requireFullRefresh();
    cache.refresh(_controller.getHelixDataAccessor());
    for (String stateModelDefName : cache.getStateModelDefMap().keySet()) {
      Assert.assertEquals(cache.getAbnormalStateResolver(stateModelDefName).getClass(),
          stateModelDefName.equals(MasterSlaveSMD.name) ?
              MockAbnormalStateResolver.class :
              AbnormalStateResolver.DUMMY_STATE_RESOLVER.getClass());
    }

    // Reset the resolver map
    clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setAbnormalStateResolverMap(Collections.emptyMap());
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }
}
