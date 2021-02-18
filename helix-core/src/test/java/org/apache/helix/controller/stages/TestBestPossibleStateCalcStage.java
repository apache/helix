package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestBestPossibleStateCalcStage extends BaseStageTest {
  @Test
  public void testSimple() {
    System.out.println("START TestBestPossibleStateCalcStage at "
        + new Date(System.currentTimeMillis()));
    // List<IdealState> idealStates = new ArrayList<IdealState>();

    String[] resources = new String[] {
      "testResourceName"
    };

    int numPartition = 5;
    int numReplica = 1;

    setupIdealState(5, resources, numPartition, numReplica, RebalanceMode.SEMI_AUTO,
        BuiltInStateModelDefinitions.MasterSlave.name());
    setupLiveInstances(5);
    setupStateModel();

    Map<String, Resource> resourceMap =
        getResourceMap(resources, numPartition, BuiltInStateModelDefinitions.MasterSlave.name());
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), new ResourceControllerDataProvider());

    ReadClusterDataStage stage1 = new ReadClusterDataStage();
    runStage(event, stage1);
    BestPossibleStateCalcStage stage2 = new BestPossibleStateCalcStage();
    runStage(event, stage2);

    BestPossibleStateOutput output =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    for (int p = 0; p < 5; p++) {
      Partition resource = new Partition("testResourceName_" + p);
      Assert.assertEquals("MASTER", output.getInstanceStateMap("testResourceName", resource)
          .get("localhost_" + (p + 1) % 5));
    }
    System.out.println("END TestBestPossibleStateCalcStage at "
        + new Date(System.currentTimeMillis()));
  }

  /*
   * Tests the pipeline detects offline instances exceed the threshold and auto enters maintenance,
   * the maintenance rebalancer is used immediately. No bootstraps in the best possible output.
   */
  @Test
  public void testAutoEnterMaintenanceWhenExceedingOfflineNodes() {
    String[] resources = new String[]{"testResourceName"};
    int numInstances = 3;
    int numPartitions = 3;

    setupIdealState(numInstances, resources, numPartitions, 1, RebalanceMode.FULL_AUTO,
        BuiltInStateModelDefinitions.MasterSlave.name());
    setupInstances(numInstances);
    List<String> liveInstances = setupLiveInstances(numInstances);
    setupStateModel();

    // Set offline instances threshold
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setMaxOfflineInstancesAllowed(1);
    setClusterConfig(clusterConfig);

    Map<String, Resource> resourceMap =
        getResourceMap(resources, numPartitions, BuiltInStateModelDefinitions.MasterSlave.name());
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    for (int p = 0; p < numPartitions; p++) {
      Partition partition = new Partition("testResourceName_" + p);
      currentStateOutput
          .setCurrentState("testResourceName", partition, "localhost_" + (p + 1) % numInstances,
              "MASTER");
    }

    // Disable 2 instances so the pipeline should enter maintenance
    for (int i = 0; i < 2; i++) {
      admin.enableInstance(_clusterName, liveInstances.get(i), false);
    }

    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new BestPossibleStateCalcStage());

    BestPossibleStateOutput output = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());

    // State on the disabled instances should be OFFLINE instead of DROPPED
    // because of maintenance rebalancer.
    Assert.assertEquals(
        output.getInstanceStateMap("testResourceName", new Partition("testResourceName_2"))
            .get("localhost_0"),
        "OFFLINE",
        "Actual state should not be DROPPED");

    Assert.assertEquals(
        output.getInstanceStateMap("testResourceName", new Partition("testResourceName_0"))
            .get("localhost_1"),
        "OFFLINE",
        "Actual state should not be DROPPED");

    // No state change for localhost_2 because the replica is already MASTER
    Assert.assertNull(
        output.getInstanceStateMap("testResourceName", new Partition("testResourceName_1"))
            .get("localhost_2"));
  }
}
