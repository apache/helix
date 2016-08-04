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
import java.util.Map;

import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

public class TestInvalidResourceRebalance extends ZkUnitTestBase {
  /**
   * Ensure that the Helix controller doesn't attempt to rebalance resources with invalid ideal
   * states
   */
  @Test
  public void testResourceRebalanceSkipped() throws Exception {
    final int NUM_PARTICIPANTS = 2;
    final int NUM_PARTITIONS = 4;
    final int NUM_REPLICAS = 2;
    final String RESOURCE_NAME = "TestDB0";

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // Set up cluster
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        NUM_PARTITIONS, // partitions per resource
        NUM_PARTICIPANTS, // number of nodes
        NUM_REPLICAS, // replicas
        "MasterSlave", RebalanceMode.SEMI_AUTO, // use SEMI_AUTO mode
        true); // do rebalance

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    // add the ideal state spec (prevents non-CUSTOMIZED MasterSlave ideal states)
    HelixAdmin helixAdmin = controller.getClusterManagmentTool();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("IdealStateRule!sampleRuleName",
        "IDEAL_STATE_MODE=CUSTOMIZED,STATE_MODEL_DEF_REF=MasterSlave");
    helixAdmin.setConfig(
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(clusterName).build(),
        properties);

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      final String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    Thread.sleep(1000);
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new EmptyZkVerifier(clusterName, RESOURCE_NAME));
    Assert.assertTrue(result, "External view and current state must be empty");

    // cleanup
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      participants[i].syncStop();
    }
    controller.syncStop();
  }

}
