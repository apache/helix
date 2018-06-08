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

import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestClusterStartsup extends ZkStandAloneCMTestBase {
  void setupCluster() throws HelixException {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, 20, STATE_MODEL);
    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = "localhost_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 3);
  }

  @Override
  @BeforeClass()
  public void beforeClass() throws Exception {

  }

  @Test()
  public void testParticipantStartUp() throws Exception {
    setupCluster();
    String controllerMsgPath = PropertyPathBuilder.controllerMessage(CLUSTER_NAME);
    _gZkClient.deleteRecursively(controllerMsgPath);
    HelixManager manager = null;

    try {
      manager =
          HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "localhost_" + (START_PORT + 1),
              InstanceType.PARTICIPANT, ZK_ADDR);
      manager.connect();
      Assert.fail("Should fail on connect() since cluster structure is not set up");
    } catch (HelixException e) {
      // OK
    }

    if (manager != null) {
      AssertJUnit.assertFalse(manager.isConnected());
    }

    try {
      manager =
          HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "localhost_" + (START_PORT + 3),
              InstanceType.PARTICIPANT, ZK_ADDR);
      manager.connect();
      Assert.fail("Should fail on connect() since cluster structure is not set up");
    } catch (HelixException e) {
      // OK
    }

    if (manager != null) {
      AssertJUnit.assertFalse(manager.isConnected());
    }

    setupCluster();
    String stateModelPath = PropertyPathBuilder.stateModelDef(CLUSTER_NAME);
    _gZkClient.deleteRecursively(stateModelPath);

    try {
      manager =
          HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "localhost_" + (START_PORT + 1),
              InstanceType.PARTICIPANT, ZK_ADDR);
      manager.connect();
      Assert.fail("Should fail on connect() since cluster structure is not set up");
    } catch (HelixException e) {
      // OK
    }
    if (manager != null) {
      AssertJUnit.assertFalse(manager.isConnected());
    }

    setupCluster();
    String instanceStatusUpdatePath =
        PropertyPathBuilder.instanceStatusUpdate(CLUSTER_NAME, "localhost_" + (START_PORT + 1));
    _gZkClient.deleteRecursively(instanceStatusUpdatePath);

    try {
      manager =
          HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "localhost_" + (START_PORT + 1),
              InstanceType.PARTICIPANT, ZK_ADDR);
      manager.connect();
      Assert.fail("Should fail on connect() since cluster structure is not set up");
    } catch (HelixException e) {
      // OK
    }
    if (manager != null) {
      AssertJUnit.assertFalse(manager.isConnected());
    }

    if (manager != null) {
      manager.disconnect();
    }
  }
}
