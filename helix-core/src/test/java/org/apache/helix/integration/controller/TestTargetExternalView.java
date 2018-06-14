package org.apache.helix.integration.controller;

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

import java.util.List;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTargetExternalView extends TaskTestBase {

  private ConfigAccessor _configAccessor;
  private HelixDataAccessor _accessor;
  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 3;
    _numParitions = 8;
    _numNodes = 4;
    _numReplicas = 2;
    super.beforeClass();
    _configAccessor = new ConfigAccessor(_gZkClient);
    _accessor = _manager.getHelixDataAccessor();
  }

  @Test
  public void testTargetExternalViewEnable() throws InterruptedException {
    // Before enable target external view
    Assert
        .assertFalse(_gZkClient.exists(_accessor.keyBuilder().targetExternalViews().getPath()));

    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.enableTargetExternalView(true);
    clusterConfig.setPersistIntermediateAssignment(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    _gSetupTool.getClusterManagementTool().rebalance(CLUSTER_NAME, _testDbs.get(0), 3);

    ZkHelixClusterVerifier verifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    Assert
        .assertEquals(_accessor.getChildNames(_accessor.keyBuilder().targetExternalViews()).size(),
            3);

    List<ExternalView> targetExternalViews =
        _accessor.getChildValues(_accessor.keyBuilder().externalViews());
    List<IdealState> idealStates = _accessor.getChildValues(_accessor.keyBuilder().idealStates());

    for (int i = 0; i < idealStates.size(); i++) {
      Assert.assertEquals(targetExternalViews.get(i).getRecord().getMapFields(),
          idealStates.get(i).getRecord().getMapFields());
      Assert.assertEquals(targetExternalViews.get(i).getRecord().getListFields(),
          idealStates.get(i).getRecord().getListFields());
    }

    // Disable one instance to see whether the target external views changes.
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, _participants[0].getInstanceName(), false);

    Assert.assertTrue(verifier.verifyByPolling());
    Thread.sleep(1000);
    targetExternalViews = _accessor.getChildValues(_accessor.keyBuilder().externalViews());
    idealStates = _accessor.getChildValues(_accessor.keyBuilder().idealStates());

    for (int i = 0; i < idealStates.size(); i++) {
      Assert.assertEquals(targetExternalViews.get(i).getRecord().getMapFields(),
          idealStates.get(i).getRecord().getMapFields());
      Assert.assertEquals(targetExternalViews.get(i).getRecord().getListFields(),
          idealStates.get(i).getRecord().getListFields());
    }
  }
}
