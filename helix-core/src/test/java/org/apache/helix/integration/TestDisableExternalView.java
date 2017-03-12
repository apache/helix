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
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test disable external-view in resource ideal state -
 * if DISABLE_EXTERNAL_VIEW is set to true in a resource's idealstate,
 * there should be no external view for this resource.
 */
public class TestDisableExternalView extends ZkIntegrationTestBase {
  private static final String TEST_DB1 = "test_db1";
  private static final String TEST_DB2 = "test_db2";

  private static final int NODE_NR = 5;
  private static final int START_PORT = 12918;
  private static final String STATE_MODEL = "MasterSlave";
  private static final int _PARTITIONS = 20;

  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  private MockParticipantManager[] _participants = new MockParticipantManager[NODE_NR];
  private ClusterControllerManager _controller;
  private String [] instances = new String[NODE_NR];

  private ZKHelixAdmin _admin;

  int _replica = 3;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _admin = new ZKHelixAdmin(_gZkClient);
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB1, _PARTITIONS, STATE_MODEL,
        IdealState.RebalanceMode.FULL_AUTO + "");

    IdealState idealState = _admin.getResourceIdealState(CLUSTER_NAME, TEST_DB1);
    idealState.setDisableExternalView(true);
    _admin.setResourceIdealState(CLUSTER_NAME, TEST_DB1, idealState);

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB2, _PARTITIONS, STATE_MODEL,
        IdealState.RebalanceMode.FULL_AUTO + "");

    for (int i = 0; i < NODE_NR; i++) {
      instances[i] = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instances[i]);
    }

    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB1, _replica);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB2, _replica);

    // start dummy participants
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      participant.syncStart();
      _participants[i] = participant;

    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    boolean result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);
  }

  @Test
  public void testDisableExternalView() throws InterruptedException {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // verify external view for TEST_DB1 does not exist
    ExternalView externalView = null;
    externalView = accessor.getProperty(keyBuilder.externalView(TEST_DB1));
    Assert.assertNull(externalView,
        "There should be no external-view for " + TEST_DB1 + ", but is: " + externalView);

    // verify external view for TEST_DB2 exists
    externalView = accessor.getProperty(keyBuilder.externalView(TEST_DB2));
    Assert.assertNotNull(externalView,
        "Could not find external-view for " + TEST_DB2);

    // disable external view in IS
    IdealState idealState = _admin.getResourceIdealState(CLUSTER_NAME, TEST_DB2);
    idealState.setDisableExternalView(true);
    _admin.setResourceIdealState(CLUSTER_NAME, TEST_DB2, idealState);

    // touch liveinstance to trigger externalview compute stage
    String instance = PARTICIPANT_PREFIX + "_" + START_PORT;
    HelixProperty liveInstance = accessor.getProperty(keyBuilder.liveInstance(instance));
    accessor.setProperty(keyBuilder.liveInstance(instance), liveInstance);

    // verify the external view for the db got removed
    for (int i = 0; i < 10; i++) {
      Thread.sleep(100);
      externalView = accessor.getProperty(keyBuilder.externalView(TEST_DB2));
      if (externalView == null) {
        break;
      }
    }

    Assert.assertNull(externalView, "external-view for " + TEST_DB2 + " should be removed, but was: "
        + externalView);
  }

  @AfterClass
  public void afterClass() {
    // clean up
    _controller.syncStop();
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].syncStop();
    }
  }

}
