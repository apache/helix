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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;
import java.util.Map;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.common.execution.TestClusterParameters;
import org.apache.helix.common.execution.TestExecutionFlow;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSchemataSM extends ZkTestBase {

  @Test
  public void testSchemataSM() {
    int n = 5;
    TestExecutionFlow flow = createClusterTestExecutionFlow()
        .createCluster(new TestClusterParameters.Builder()
            .setResourcesCount(1)
            .setResourceName("TestSchemata")
            .setPartitionsPerResource(1)
            .setNodeCount(n)
            .setReplicaCount(0)
            .setStateModelDef("STORAGE_DEFAULT_SM_SCHEMATA")
            .disableRebalance()
            .build());

    String clusterName = flow.getClusterName();

    // rebalance ideal-state to use ANY_LIVEINSTANCE for preference list
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    PropertyKey key = keyBuilder.idealStates("TestSchemata0");
    IdealState idealState = accessor.getProperty(key);
    idealState.setReplicas(IdealState.IdealStateConstants.ANY_LIVEINSTANCE.toString());
    idealState.getRecord().setListField("TestSchemata0_0",
        Collections.singletonList(IdealState.IdealStateConstants.ANY_LIVEINSTANCE.toString()));
    accessor.setProperty(key, idealState);

    flow.startController();
    MockParticipantManager[] participants = flow.getParticipants();

    // start n-1 participants
    for (int i = 1; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    flow.assertBestPossibleExternalViewVerifier();

    // start the remaining 1 participant
    participants[0] = new MockParticipantManager(ZK_ADDR, clusterName, "localhost_12918");
    participants[0].syncStart();

    // make sure we have all participants in MASTER state
    flow.assertBestPossibleExternalViewVerifier();

    key = keyBuilder.externalView("TestSchemata0");
    ExternalView externalView = accessor.getProperty(key);
    Map<String, String> stateMap = externalView.getStateMap("TestSchemata0_0");
    Assert.assertNotNull(stateMap);
    Assert.assertEquals(stateMap.size(), n, "all " + n + " participants should be in Master state");
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      Assert.assertNotNull(stateMap.get(instanceName));
      Assert.assertEquals(stateMap.get(instanceName), "MASTER");
    }

    flow.cleanup();
  }
}
