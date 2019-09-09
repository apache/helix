package org.apache.helix.controller.rebalancer.waged;

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

import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ResourceAssignment;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAssignmentMetadataStore extends ZkTestBase {
  protected static final int NODE_NR = 5;
  protected static final int START_PORT = 12918;
  protected static final String STATE_MODEL = "MasterSlave";
  protected static final String TEST_DB = "TestDB";
  protected static final int _PARTITIONS = 20;

  protected HelixManager _manager;
  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  protected MockParticipantManager[] _participants = new MockParticipantManager[NODE_NR];
  protected ClusterControllerManager _controller;
  protected int _replica = 3;

  private AssignmentMetadataStore _store;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, _PARTITIONS, STATE_MODEL);
    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, _replica);

    // start dummy participants
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // create cluster manager
    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    // create AssignmentMetadataStore
    _store = new AssignmentMetadataStore(_manager);
  }

  /**
   * TODO: Reading baseline will be empty because AssignmentMetadataStore isn't being used yet by
   * the new rebalancer. Modify this integration test once the WAGED rebalancer
   * starts using AssignmentMetadataStore's persist APIs.
   * TODO: WAGED Rebalancer currently does NOT work with ZKClusterVerifier because verifier's
   * HelixManager is null, and that causes an NPE when instantiating AssignmentMetadataStore.
   */
  @Test
  public void testReadEmptyBaseline() {
    try {
      Map<String, ResourceAssignment> baseline = _store.getBaseline();
      Assert.fail("Should fail because there shouldn't be any data.");
    } catch (Exception e) {
      // OK
    }
  }
}
