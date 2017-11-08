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

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;

// Helix-50: integration test for generate message based on state priority
public class TestInvalidAutoIdealState extends ZkUnitTestBase {
  // TODO Disable this test, need refactor it for testing message generation based on state priority
  // @Test
  void testInvalidReplica2() throws Exception {
    HelixAdmin admin = new ZKHelixAdmin(ZK_ADDR);

    // create cluster
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    String db = "TestDB";

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // System.out.println("Creating cluster: " + clusterName);
    admin.addCluster(clusterName, true);

    // add MasterSlave state mode definition
    admin.addStateModelDef(clusterName, "MasterSlave", new StateModelDefinition(
        StateModelConfigGenerator.generateConfigForMasterSlave()));

    // Add nodes to the cluster
    int n = 3;
    System.out.println("Adding " + n + " participants to the cluster");
    for (int i = 0; i < n; i++) {
      int port = 12918 + i;
      InstanceConfig instanceConfig = new InstanceConfig("localhost_" + port);
      instanceConfig.setHostName("localhost");
      instanceConfig.setPort("" + port);
      instanceConfig.setInstanceEnabled(true);
      admin.addInstance(clusterName, instanceConfig);
      // System.out.println("\t Added participant: " + instanceConfig.getInstanceName());
    }

    // construct ideal-state manually
    IdealState idealState = new IdealState(db);
    idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
    idealState.setNumPartitions(2);
    idealState.setReplicas("" + 2); // should be 3
    idealState.setStateModelDefRef("MasterSlave");
    idealState.getRecord().setListField("TestDB_0",
        Arrays.asList("localhost_12918", "localhost_12919", "localhost_12920"));
    idealState.getRecord().setListField("TestDB_1",
        Arrays.asList("localhost_12919", "localhost_12918", "localhost_12920"));

    admin.setResourceIdealState(clusterName, "TestDB", idealState);

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // make sure localhost_12919 is master on TestDB_1
    HelixDataAccessor accessor = controller.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    ExternalView extView = accessor.getProperty(keyBuilder.externalView(db));
    Map<String, String> stateMap = extView.getStateMap(db + "_1");
    Assert
        .assertEquals(
            stateMap.get("localhost_12919"),
            "MASTER",
            "localhost_12919 should be MASTER even though replicas is set to 2, since we generate message based on target-state priority");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
