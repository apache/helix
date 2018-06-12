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
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterDistributedController;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAddClusterV2 extends ZkTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestAddClusterV2.class);

  protected static final int CLUSTER_NR = 10;
  protected static final int NODE_NR = 5;
  protected static final int START_PORT = 12918;
  protected static final String STATE_MODEL = "MasterSlave";

  protected final String CLASS_NAME = getShortClassName();
  protected final String CONTROLLER_CLUSTER = CONTROLLER_CLUSTER_PREFIX + "_" + CLASS_NAME;

  protected static final String TEST_DB = "TestDB";

  MockParticipantManager[] _participants = new MockParticipantManager[NODE_NR];
  ClusterDistributedController[] _distControllers = new ClusterDistributedController[NODE_NR];

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    // setup CONTROLLER_CLUSTER
    _gSetupTool.addCluster(CONTROLLER_CLUSTER, true);
    for (int i = 0; i < NODE_NR; i++) {
      String controllerName = CONTROLLER_PREFIX + "_" + i;
      _gSetupTool.addInstanceToCluster(CONTROLLER_CLUSTER, controllerName);
    }

    // setup cluster of clusters
    for (int i = 0; i < CLUSTER_NR; i++) {
      String clusterName = CLUSTER_PREFIX + "_" + CLASS_NAME + "_" + i;
      _gSetupTool.addCluster(clusterName, true);
      _gSetupTool.activateCluster(clusterName, CONTROLLER_CLUSTER, true);
    }

    final String firstCluster = CLUSTER_PREFIX + "_" + CLASS_NAME + "_0";
    setupStorageCluster(_gSetupTool, firstCluster, TEST_DB, 20, PARTICIPANT_PREFIX, START_PORT,
        "MasterSlave", 3, true);

    // start dummy participants for the first cluster
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, firstCluster, instanceName);
      _participants[i].syncStart();
    }

    // start distributed cluster controllers
    for (int i = 0; i < NODE_NR; i++) {
      String controllerName = CONTROLLER_PREFIX + "_" + i;
      _distControllers[i] =
          new ClusterDistributedController(ZK_ADDR, CONTROLLER_CLUSTER, controllerName);
      _distControllers[i].syncStart();
    }

    verifyClusters();
  }

  @Test
  public void Test() {

  }

  @AfterClass
  public void afterClass() throws Exception {
    System.out.println("AFTERCLASS " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    /**
     * shutdown order:
     * 1) pause the leader (optional)
     * 2) disconnect all controllers
     * 3) disconnect leader/disconnect participant
     */
    String leader = getCurrentLeader(_gZkClient, CONTROLLER_CLUSTER);
    int leaderIdx = -1;
    for (int i = 0; i < NODE_NR; i++) {
      if (!_distControllers[i].getInstanceName().equals(leader)) {
        _distControllers[i].syncStop();
        verifyClusters();
      } else {
        leaderIdx = i;
      }
    }
    Assert.assertNotSame(leaderIdx, -1);

    _distControllers[leaderIdx].syncStop();

    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].syncStop();
    }

    // delete clusters
    for (int i = 0; i < CLUSTER_NR; i++) {
      String clusterName = CLUSTER_PREFIX + "_" + CLASS_NAME + "_" + i;
      _gSetupTool.deleteCluster(clusterName);
    }

    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * verify the external view (against the best possible state)
   * in the controller cluster and the first cluster
   */
  protected void verifyClusters() {
    ZkHelixClusterVerifier _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CONTROLLER_CLUSTER).setZkClient(_gZkClient)
            .build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_PREFIX + "_" + CLASS_NAME + "_0")
            .setZkClient(_gZkClient).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  protected void setupStorageCluster(ClusterSetup setupTool, String clusterName, String dbName,
      int partitionNr, String prefix, int startPort, String stateModel, int replica,
      boolean rebalance) {
    setupTool.addResourceToCluster(clusterName, dbName, partitionNr, stateModel);
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = prefix + "_" + (startPort + i);
      setupTool.addInstanceToCluster(clusterName, instanceName);
    }
    if (rebalance) {
      setupTool.rebalanceStorageCluster(clusterName, dbName, replica);
    }
  }
}
