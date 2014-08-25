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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockMultiClusterController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAddClusterV2 extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(TestAddClusterV2.class);

  protected static final int CLUSTER_NR = 10;
  protected static final int NODE_NR = 5;
  protected static final int START_PORT = 12918;
  protected static final String STATE_MODEL = "MasterSlave";

  protected final String CLASS_NAME = "TestAddClusterV2";
  protected final String CONTROLLER_CLUSTER = "super_" + CLASS_NAME;

  protected static final String TEST_DB = "TestDB";

  MockParticipant[] _participants = new MockParticipant[NODE_NR];
  MockMultiClusterController[] _multiClusterControllers = new MockMultiClusterController[NODE_NR];

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CONTROLLER_CLUSTER;
    if (_zkclient.exists(namespace)) {
      _zkclient.deleteRecursive(namespace);
    }

    for (int i = 0; i < CLUSTER_NR; i++) {
      namespace = "/" + CLASS_NAME + "_" + i;
      if (_zkclient.exists(namespace)) {
        _zkclient.deleteRecursive(namespace);
      }
    }


    // setup CONTROLLER_CLUSTER
    _setupTool.addCluster(CONTROLLER_CLUSTER, true);
    for (int i = 0; i < NODE_NR; i++) {
      String controllerName = "controller_" + i;
      _setupTool.addInstanceToCluster(CONTROLLER_CLUSTER, controllerName);
    }

    // setup cluster of clusters
    for (int i = 0; i < CLUSTER_NR; i++) {
      String clusterName = CLASS_NAME + "_" + i;
      _setupTool.addCluster(clusterName, true);
      _setupTool.activateCluster(clusterName, CONTROLLER_CLUSTER, true);
    }

    final String firstCluster = CLASS_NAME + "_0";
    setupStorageCluster(_setupTool, firstCluster, TEST_DB, 20, "localhost", START_PORT,
        "MasterSlave", 3, true);

    // start dummy participants for the first cluster
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = "localhost_" + (START_PORT + i);
      _participants[i] = new MockParticipant(_zkaddr, firstCluster, instanceName);
      _participants[i].syncStart();
    }

    // start multi-cluster controllers
    for (int i = 0; i < NODE_NR; i++) {
      String controllerName = "controller_" + i;
      _multiClusterControllers[i] =
          new MockMultiClusterController(_zkaddr, CONTROLLER_CLUSTER, controllerName);
      _multiClusterControllers[i].syncStart();
    }

    verifyClusters();
  }

  private String getCurrentLeader(String clusterName) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    if (leader == null) {
      return null;
    }
    return leader.getInstanceName();
  }

  @Test
  public void test() {
    // placeholder for running beforeClass() and afterClass()
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
    String leader = getCurrentLeader(CONTROLLER_CLUSTER);
    int leaderIdx = -1;
    for (int i = 0; i < NODE_NR; i++) {
      if (!_multiClusterControllers[i].getInstanceName().equals(leader)) {
        _multiClusterControllers[i].syncStop();
        verifyClusters();
      } else {
        leaderIdx = i;
      }
    }
    Assert.assertNotSame(leaderIdx, -1);

    _multiClusterControllers[leaderIdx].syncStop();

    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].syncStop();
    }
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * verify the external view (against the best possible state)
   * in the controller cluster and the first cluster
   */
  protected void verifyClusters() {
    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, CONTROLLER_CLUSTER));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, CLASS_NAME + "_0"));
    Assert.assertTrue(result);
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
