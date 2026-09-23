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

import java.util.Map;

import java.util.Objects;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test to verify that ExternalViewComputeStage on IdealStateChange events only copies
 * simple fields  and not partition assignments from IdealState to ExternalView.
 */
public class TestExternalViewComputeOnIdealStateChange extends BaseStageTest {

  @Test
  public void testExternalViewComputeOnlySimpleFieldsFromIdealState() throws InterruptedException {
    String resourceName = "TestDB";
    String partition1 = resourceName + "_0";
    String partition2 = resourceName + "_1";
    String instance1 = "localhost_1001";
    String instance2 = "localhost_1002";
    String instance3 = "localhost_1003";

    setupStateModel();

    // create initial IdealState
    IdealState idealState = new IdealState(resourceName);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    idealState.setNumPartitions(2);
    idealState.setReplicas("2");
    idealState.setMinActiveReplicas(1);

    // Set initial simple field config
    idealState.getRecord().setSimpleField("CUSTOM_CONFIG_KEY", "initial_value");
    idealState.getRecord().setSimpleField("CUSTOM_TIMEOUT", "5000");

    // Set initial partition assignments in IdealState
    idealState.setPartitionState(partition1, instance1, "MASTER");
    idealState.setPartitionState(partition1, instance2, "SLAVE");
    idealState.setPartitionState(partition2, instance2, "MASTER");
    idealState.setPartitionState(partition2, instance3, "SLAVE");

    setSingleIdealState(idealState);

    // run pipeline stages to setup the environment
    ClusterEvent event = new ClusterEvent(_clusterName, ClusterEventType.IdealStateChange);
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(_clusterName);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    event.addAttribute(AttributeName.helixmanager.name(), manager);

    runStage(event, new ReadClusterDataStage());
    runStage(event, new ResourceComputationStage());
    runStage(event, new CurrentStateComputationStage());
    runStage(event, new ExternalViewComputeStage());

    Builder keyBuilder = accessor.keyBuilder();

    // update IdealState
    IdealState updatedIdealState = new IdealState(resourceName);
    updatedIdealState.setStateModelDefRef("MasterSlave");
    updatedIdealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    updatedIdealState.setNumPartitions(2);
    updatedIdealState.setReplicas("2");
    updatedIdealState.setMinActiveReplicas(1);

    // Update simple field config, this should get copied to ExternalView
    updatedIdealState.getRecord().setSimpleField("CUSTOM_CONFIG_KEY", "updated_value");
    updatedIdealState.getRecord().setSimpleField("CUSTOM_TIMEOUT", "10000");
    updatedIdealState.getRecord().setSimpleField("NEW_CONFIG", "new_config_value");

    // Update partition assignments, this shouldn't get copied to ExternalView
    updatedIdealState.setPartitionState(partition1, instance3, "MASTER");
    updatedIdealState.setPartitionState(partition1, instance1, "SLAVE");
    updatedIdealState.setPartitionState(partition2, instance1, "MASTER");
    updatedIdealState.setPartitionState(partition2, instance2, "SLAVE");

    setSingleIdealState(updatedIdealState);

    cache = new ResourceControllerDataProvider(_clusterName);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    // Re-run all stages with new cache to ensure it has both updated IdealState and current state
    runStage(event, new ReadClusterDataStage());
    runStage(event, new ResourceComputationStage());
    runStage(event, new CurrentStateComputationStage());
    runStage(event, new ExternalViewComputeStage());

    // verify results
    ExternalView updatedEV = accessor.getProperty(keyBuilder.externalView(resourceName));
    System.out.println("Updated ExternalView simple fields: " + updatedEV.getRecord().getSimpleFields());
    Assert.assertNotNull(updatedEV, "ExternalView should exist after running ExternalViewComputeStage");

    Assert.assertEquals(updatedEV.getRecord().getSimpleField("CUSTOM_CONFIG_KEY"), "updated_value",
        "Simple field CUSTOM_CONFIG_KEY should be copied from IdealState");
    Assert.assertEquals(updatedEV.getRecord().getSimpleField("CUSTOM_TIMEOUT"), "10000",
        "Simple field CUSTOM_TIMEOUT should be copied from IdealState");
    Assert.assertEquals(updatedEV.getRecord().getSimpleField("NEW_CONFIG"), "new_config_value",
        "New simple field NEW_CONFIG should be copied from IdealState");

    Map<String, String> finalPartition1States = updatedEV.getStateMap(partition1);
    Map<String, String> finalPartition2States = updatedEV.getStateMap(partition2);

    Map<String, String> idealStatePartition1 = updatedIdealState.getInstanceStateMap(partition1);
    Map<String, String> idealStatePartition2 = updatedIdealState.getInstanceStateMap(partition2);

    // verify partition assignments are not equal to IdealState assignments
    Assert.assertFalse(
        Objects.equals(finalPartition1States, idealStatePartition1),
        "Final EV partition1 assignments should NOT match updated IdealState assignments"
    );
    Assert.assertFalse(
        Objects.equals(finalPartition2States, idealStatePartition2),
        "Final EV partition2 assignments should NOT match updated IdealState assignments"
    );

  }
}
