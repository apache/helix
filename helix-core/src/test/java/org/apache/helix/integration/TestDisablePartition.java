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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.controller.rebalancer.AutoRebalancer;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestDisablePartition extends ZkStandAloneCMTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestDisablePartition.class);

  @Test()
  public void testDisablePartition() throws Exception {
    LOG.info("START testDisablePartition() at " + new Date(System.currentTimeMillis()));

    // localhost_12919 is MASTER for TestDB_0
    String command = "--zkSvr " + ZK_ADDR + " --enablePartition false " + CLUSTER_NAME
        + " localhost_12919 TestDB TestDB_0 TestDB_9";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Map<String, Set<String>> map = new HashMap<>();
    map.put("TestDB_0", TestHelper.setOf("localhost_12919"));
    map.put("TestDB_9", TestHelper.setOf("localhost_12919"));

    boolean result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    TestHelper.verifyState(CLUSTER_NAME, ZK_ADDR, map, "OFFLINE");

    ZKHelixAdmin tool = new ZKHelixAdmin(_gZkClient);
    tool.enablePartition(true, CLUSTER_NAME, "localhost_12919", "TestDB",
        Collections.singletonList("TestDB_9"));

    result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    map.clear();
    map.put("TestDB_0", TestHelper.setOf("localhost_12919"));
    TestHelper.verifyState(CLUSTER_NAME, ZK_ADDR, map, "OFFLINE");

    map.clear();
    map.put("TestDB_9", TestHelper.setOf("localhost_12919"));
    TestHelper.verifyState(CLUSTER_NAME, ZK_ADDR, map, "MASTER");

    LOG.info("STOP testDisablePartition() at " + new Date(System.currentTimeMillis()));

  }

  @DataProvider(name = "rebalancer")
  public static String[][] rebalancers() {
    return new String[][] {
        {
            AutoRebalancer.class.getName()
        }, {
            DelayedAutoRebalancer.class.getName()
        }
    };
  }

  @Test(dataProvider = "rebalancer", enabled = true)
  public void testDisableFullAuto(String rebalancerName) throws Exception {
    final int NUM_PARTITIONS = 8;
    final int NUM_PARTICIPANTS = 2;
    final int NUM_REPLICAS = 1;
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    ClusterSetup clusterSetup = new ClusterSetup(ZK_ADDR);
    clusterSetup.addCluster(clusterName, true);

    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String instanceName = "localhost_" + (11420 + i);
      clusterSetup.addInstanceToCluster(clusterName, instanceName);
    }

    // Create a known problematic scenario
    HelixAdmin admin = clusterSetup.getClusterManagementTool();
    String resourceName = "MailboxDB";
    IdealState idealState = new IdealState(resourceName + "DR");
    idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
    idealState.setStateModelDefRef("LeaderStandby");
    idealState.setReplicas(String.valueOf(NUM_REPLICAS));
    idealState.setNumPartitions(NUM_PARTITIONS);
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      String partitionName = resourceName + '_' + i;
      List<String> assignmentList = Lists.newArrayList();
      if (i < NUM_PARTITIONS / 2) {
        assignmentList.add("localhost_11420");
      } else {
        assignmentList.add("localhost_11421");
      }
      Map<String, String> emptyMap = Maps.newHashMap();
      idealState.getRecord().setListField(partitionName, assignmentList);
      idealState.getRecord().setMapField(partitionName, emptyMap);
    }
    admin.addResource(clusterName, idealState.getResourceName(), idealState);

    // Start everything
    MockParticipantManager[] participants = new MockParticipantManager[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String instanceName = "localhost_" + (11420 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_1");
    controller.syncStart();

    Thread.sleep(1000);

    // Switch to full auto
    idealState.setRebalanceMode(RebalanceMode.FULL_AUTO);
    idealState.setRebalancerClassName(rebalancerName);
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      List<String> emptyList = Collections.emptyList();
      idealState.getRecord().setListField(resourceName + '_' + i, emptyList);
    }
    admin.setResourceIdealState(clusterName, idealState.getResourceName(), idealState);

    Thread.sleep(1000);

    // Get the external view
    HelixDataAccessor accessor = controller.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    ExternalView externalView =
        accessor.getProperty(keyBuilder.externalView(idealState.getResourceName()));

    // Disable the partitions in an order known to cause problems
    int[] pid = {
        0, 7
    };
    for (int value : pid) {
      String partitionName = resourceName + '_' + value;
      Map<String, String> stateMap = externalView.getStateMap(partitionName);
      String leader = null;
      for (String participantName : stateMap.keySet()) {
        String state = stateMap.get(participantName);
        if (state.equals("LEADER")) {
          leader = participantName;
        }
      }
      List<String> partitionNames = Lists.newArrayList(partitionName);
      admin.enablePartition(false, clusterName, leader, idealState.getResourceName(), partitionNames);

      Thread.sleep(1000);
    }

    // Ensure that nothing was reassigned and the disabled are offline
    externalView = accessor.getProperty(keyBuilder.externalView(idealState.getResourceName()));
    Map<String, String> p0StateMap = externalView.getStateMap(resourceName + "_0");
    Assert.assertEquals(p0StateMap.size(), 1);
    String p0Participant = p0StateMap.keySet().iterator().next();
    Assert.assertEquals(p0StateMap.get(p0Participant), "OFFLINE");
    Map<String, String> p7StateMap = externalView.getStateMap(resourceName + "_7");
    Assert.assertEquals(p7StateMap.size(), 1);
    String p7Participant = p7StateMap.keySet().iterator().next();
    Assert.assertEquals(p7StateMap.get(p7Participant), "OFFLINE");

    // Cleanup
    controller.syncStop();
    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }

    deleteCluster(clusterName);
  }
}
