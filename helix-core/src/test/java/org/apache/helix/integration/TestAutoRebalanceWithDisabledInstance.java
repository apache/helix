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

import org.apache.helix.HelixAdmin;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterStateVerifier.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestAutoRebalanceWithDisabledInstance extends ZkStandAloneCMTestBase {
  private static String TEST_DB_2 = "TestDB2";

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();
    _setupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB_2, _PARTITIONS, STATE_MODEL,
        RebalanceMode.FULL_AUTO + "");
    _setupTool.rebalanceResource(CLUSTER_NAME, TEST_DB_2, _replica);

    Thread.sleep(200);

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
            CLUSTER_NAME));
    Assert.assertTrue(result);
  }

  @Test()
  public void testDisableEnableInstanceAutoRebalance() throws Exception {
    String disabledInstance = _participants[0].getInstanceName();

    Set<String> currentPartitions = getCurrentPartitionsOnInstance(CLUSTER_NAME, TEST_DB_2,
        disabledInstance);
    Assert.assertFalse(currentPartitions.isEmpty());

    // disable instance
    _setupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, disabledInstance, false);
    Thread.sleep(1000);
    currentPartitions = getCurrentPartitionsOnInstance(CLUSTER_NAME, TEST_DB_2, disabledInstance);
    Assert.assertTrue(currentPartitions.isEmpty());

    //enable instance
    _setupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, disabledInstance, true);
    Thread.sleep(1000);
    currentPartitions = getCurrentPartitionsOnInstance(CLUSTER_NAME, TEST_DB_2, disabledInstance);
    Assert.assertFalse(currentPartitions.isEmpty());
  }

  @Test()
  public void testAddDisabledInstanceAutoRebalance() throws Exception {
    // add disabled instance.
    String nodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + NODE_NR);
    _setupTool.addInstanceToCluster(CLUSTER_NAME, nodeName);
    MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, nodeName);
    _setupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, nodeName, false);

    participant.syncStart();

    Thread.sleep(400);
    Set<String> currentPartitions = getCurrentPartitionsOnInstance(CLUSTER_NAME, TEST_DB_2,
        nodeName);
    Assert.assertTrue(currentPartitions.isEmpty());

    //enable instance
    _setupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, nodeName, true);
    Thread.sleep(400);
    currentPartitions = getCurrentPartitionsOnInstance(CLUSTER_NAME, TEST_DB_2, nodeName);
    Assert.assertFalse(currentPartitions.isEmpty());
  }


  private Set<String> getCurrentPartitionsOnInstance(String cluster, String dbName, String instance) {
    HelixAdmin admin = _setupTool.getClusterManagementTool();
    Set<String> partitionSet = new HashSet<String>();

    ExternalView ev = admin.getResourceExternalView(cluster, dbName);
    for (String partition : ev.getRecord().getMapFields().keySet()) {
      Map<String, String> assignments = ev.getRecord().getMapField(partition);
      for (String ins : assignments.keySet()) {
        if (ins.equals(instance)) {
          partitionSet.add(partition);
        }
      }
    }
    return partitionSet;
  }
}
