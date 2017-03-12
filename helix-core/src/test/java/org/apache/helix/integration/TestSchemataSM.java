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

import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.ClusterStateVerifier;
import org.apache.helix.tools.ClusterVerifiers.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSchemataSM extends ZkIntegrationTestBase {
  @Test
  public void testSchemataSM() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 5;

    MockParticipantManager[] participants = new MockParticipantManager[n];

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant start port
        "localhost", // participant name prefix
        "TestSchemata", // resource name prefix
        1, // resources
        1, // partitions per resource
        n, // number of nodes
        0, // replicas
        "STORAGE_DEFAULT_SM_SCHEMATA", false); // don't rebalance

    // rebalance ideal-state to use ANY_LIVEINSTANCE for preference list
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    PropertyKey key = keyBuilder.idealStates("TestSchemata0");
    IdealState idealState = accessor.getProperty(key);
    idealState.setReplicas(IdealState.IdealStateConstants.ANY_LIVEINSTANCE.toString());
    idealState.getRecord().setListField("TestSchemata0_0",
        Arrays.asList(IdealState.IdealStateConstants.ANY_LIVEINSTANCE.toString()));
    accessor.setProperty(key, idealState);

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    // start n-1 participants
    for (int i = 1; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // start the remaining 1 participant
    participants[0] = new MockParticipantManager(ZK_ADDR, clusterName, "localhost_12918");
    participants[0].syncStart();

    // make sure we have all participants in MASTER state
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);
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

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
