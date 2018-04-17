package org.apache.helix.integration.rebalancer.PartitionMigration;

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
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestExpandCluster extends TestPartitionMigrationBase {

  Map<String, IdealState> _resourceMap;


  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _resourceMap = createTestDBs(1000000);
    _migrationVerifier = new MigrationStateVerifier(_resourceMap, _manager);
  }

  @Test
  public void testClusterExpansion() throws Exception {
    Assert.assertTrue(_clusterVerifier.verify());

    _migrationVerifier.start();

    // expand cluster by adding instance one by one
    int numNodes = _participants.size();
    for (int i = numNodes; i < numNodes + NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      MockParticipantManager participant = createAndStartParticipant(storageNodeName);
      _participants.add(participant);
      Thread.sleep(50);
    }

    Assert.assertTrue(_clusterVerifier.verify());
    Assert.assertFalse(_migrationVerifier.hasLessReplica());
    Assert.assertFalse(_migrationVerifier.hasMoreReplica());

    _migrationVerifier.stop();
  }


  @Test (dependsOnMethods = {"testClusterExpansion"})
  public void testClusterExpansionByEnableInstance() throws Exception {
    Assert.assertTrue(_clusterVerifier.verify());

    _migrationVerifier.reset();
    _migrationVerifier.start();

    int numNodes = _participants.size();
    // add new instances with all disabled
    for (int i = numNodes; i < numNodes + NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      InstanceConfig config = InstanceConfig.toInstanceConfig(storageNodeName);
      config.setInstanceEnabled(false);

      _setupTool.getClusterManagementTool().addInstance(CLUSTER_NAME, config);

      // start dummy participants
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName);
      participant.syncStart();
      _participants.add(participant);
    }

    // enable new instance one by one
    for (int i = numNodes; i < numNodes + NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, storageNodeName, true);
      Thread.sleep(100);
    }

    Assert.assertTrue(_clusterVerifier.verify());
    Assert.assertFalse(_migrationVerifier.hasLessReplica());
    Assert.assertFalse(_migrationVerifier.hasMoreReplica());

    _migrationVerifier.stop();
  }

  @Test(dependsOnMethods = {"testClusterExpansion", "testClusterExpansionByEnableInstance"})
  public void testClusterShrink() throws Exception {
    Assert.assertTrue(_clusterVerifier.verify());

    _migrationVerifier.reset();
    _migrationVerifier.start();

    // remove instance one by one
    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      MockParticipantManager participant = _participants.get(i);
      participant.syncStop();
      _setupTool.dropInstanceFromCluster(CLUSTER_NAME, storageNodeName);
    }

    Assert.assertTrue(_clusterVerifier.verify());
    Assert.assertFalse(_migrationVerifier.hasLessReplica());
    Assert.assertFalse(_migrationVerifier.hasMoreReplica());

    _migrationVerifier.stop();
  }
}
