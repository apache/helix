package org.apache.helix.view.integration;

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

import java.util.ArrayList;
import java.util.List;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class ViewAggregatorIntegrationTestBase extends ZkTestBase {
  protected static final int numSourceCluster = 2;
  protected static final int numParticipant = 3;
  protected static final String testSourceClusterNamePrefix = "testSourceCluster";
  protected static final String testParticipantNamePrefix = "testParticipant";
  protected static final String testControllerNamePrefix = "testController";

  protected List<String> _allSourceClusters = new ArrayList<>();
  protected List<MockParticipantManager> _allParticipants = new ArrayList<>();
  protected List<ClusterControllerManager> _allControllers = new ArrayList<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    for (int i = 0; i < getNumSourceCluster(); i++) {
      // Setup cluster
      String clusterName =
          String.format("%s-%s-%s", testSourceClusterNamePrefix, this.hashCode(), i);
      _gSetupTool.addCluster(clusterName, false);
      // Setup participants
      for (int j = 0; j < numParticipant; j++) {
        String instanceName = String.format("%s-%s-%s", clusterName, testParticipantNamePrefix, j);
        _gSetupTool.addInstanceToCluster(clusterName, instanceName);
        MockParticipantManager participant =
            new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
        participant.syncStart();
        _allParticipants.add(participant);
      }

      // Setup controller
      ClusterControllerManager controller = new ClusterControllerManager(ZK_ADDR, clusterName,
          String.format("%s-%s", testControllerNamePrefix, clusterName));
      controller.syncStart();
      _allControllers.add(controller);
      _allSourceClusters.add(clusterName);
    }
  }

  @AfterClass
  public void afterClass() throws Exception {
    // Stop all controllers
    for (ClusterControllerManager controller : _allControllers) {
      if (controller.isConnected()) {
        controller.syncStop();
      }
    }

    // Stop all participants
    for (MockParticipantManager participant : _allParticipants) {
      if (participant.isConnected()) {
        participant.syncStop();
      }
    }
  }

  protected int getNumSourceCluster() {
    return numSourceCluster;
  }
}
