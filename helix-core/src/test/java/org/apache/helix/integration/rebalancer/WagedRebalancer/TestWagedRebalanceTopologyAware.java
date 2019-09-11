package org.apache.helix.integration.rebalancer.WagedRebalancer;

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

import org.apache.helix.ConfigAccessor;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestWagedRebalanceTopologyAware extends TestWagedRebalanceFaultZone {
  private static final String TOLOPOGY_DEF = "/DOMAIN/ZONE/INSTANCE";
  private static final String DOMAIN_NAME = "Domain";
  private static final String FAULT_ZONE = "ZONE";

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopology(TOLOPOGY_DEF);
    clusterConfig.setFaultZoneType(FAULT_ZONE);
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      addInstanceConfig(storageNodeName, i, ZONES, TAGS);
    }

    // start dummy participants
    for (String node : _nodes) {
      MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
      participant.syncStart();
      _participants.add(participant);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
    enableTopologyAwareRebalance(_gZkClient, CLUSTER_NAME, true);
  }

  protected void addInstanceConfig(String storageNodeName, int seqNo, int zoneCount, int tagCount) {
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    String zone = "zone-" + seqNo % zoneCount;
    String tag = "tag-" + seqNo % tagCount;

    InstanceConfig config =
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, storageNodeName);
    config.setDomain(
        String.format("DOMAIN=%s,ZONE=%s,INSTANCE=%s", DOMAIN_NAME, zone, storageNodeName));
    config.addTag(tag);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, storageNodeName, config);

    _nodeToZoneMap.put(storageNodeName, zone);
    _nodeToTagMap.put(storageNodeName, tag);
    _nodes.add(storageNodeName);
  }

  @Test
  public void testZoneIsolation() throws Exception {
    super.testZoneIsolation();
  }

  @Test
  public void testZoneIsolationWithInstanceTag() throws Exception {
    super.testZoneIsolationWithInstanceTag();
  }

  @Test(dependsOnMethods = { "testZoneIsolation", "testZoneIsolationWithInstanceTag" })
  public void testLackEnoughLiveRacks() throws Exception {
    super.testLackEnoughLiveRacks();
  }

  @Test(dependsOnMethods = { "testLackEnoughLiveRacks" })
  public void testLackEnoughRacks() throws Exception {
    super.testLackEnoughRacks();
  }

  @Test(dependsOnMethods = { "testZoneIsolation", "testZoneIsolationWithInstanceTag" })
  public void testAddZone() throws Exception {
    super.testAddZone();
  }
}
