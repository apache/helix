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
import java.util.Map;
import java.util.UUID;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.strategy.DefaultTwoStateStrategy;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestResourceComputationStage extends BaseStageTest {
  /**
   * Case where we have one resource in IdealState
   * @throws Exception
   */
  @Test
  public void testSimple() throws Exception {
    int nodes = 5;
    List<String> instances = new ArrayList<String>();
    for (int i = 0; i < nodes; i++) {
      instances.add("localhost_" + i);
    }
    int partitions = 10;
    int replicas = 1;
    String resourceName = "testResource";
    ZNRecord record =
        DefaultTwoStateStrategy.calculateIdealState(instances, partitions, replicas, resourceName,
            "MASTER", "SLAVE");
    IdealState idealState = new IdealState(record);
    idealState.setStateModelDefId(StateModelDefId.from("MasterSlave"));

    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.idealStates(resourceName), idealState);
    ResourceComputationStage stage = new ResourceComputationStage();
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);

    Map<ResourceId, ResourceConfig> resource =
        event.getAttribute(AttributeName.RESOURCES.toString());
    AssertJUnit.assertEquals(1, resource.size());

    AssertJUnit.assertEquals(resource.keySet().iterator().next(), ResourceId.from(resourceName));
    AssertJUnit.assertEquals(resource.values().iterator().next().getId(),
        ResourceId.from(resourceName));
    AssertJUnit.assertEquals(resource.values().iterator().next().getIdealState()
        .getStateModelDefId(), idealState.getStateModelDefId());
    AssertJUnit
        .assertEquals(resource.values().iterator().next().getSubUnitSet().size(), partitions);
  }

  @Test
  public void testMultipleResources() throws Exception {
    // List<IdealState> idealStates = new ArrayList<IdealState>();
    String[] resources = new String[] {
        "testResource1", "testResource2"
    };
    List<IdealState> idealStates = setupIdealState(5, resources, 10, 1, RebalanceMode.SEMI_AUTO);
    ResourceComputationStage stage = new ResourceComputationStage();
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);

    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    AssertJUnit.assertEquals(resources.length, resourceMap.size());

    for (int i = 0; i < resources.length; i++) {
      String resourceName = resources[i];
      ResourceId resourceId = ResourceId.from(resourceName);
      IdealState idealState = idealStates.get(i);
      AssertJUnit.assertTrue(resourceMap.containsKey(resourceId));
      AssertJUnit.assertEquals(resourceMap.get(resourceId).getId(), resourceId);
      AssertJUnit.assertEquals(resourceMap.get(resourceId).getIdealState().getStateModelDefId(),
          idealState.getStateModelDefId());
      AssertJUnit.assertEquals(resourceMap.get(resourceId).getSubUnitSet().size(),
          idealState.getNumPartitions());
    }
  }

  @Test
  public void testMultipleResourcesWithSomeDropped() throws Exception {
    int nodes = 5;
    List<String> instances = new ArrayList<String>();
    for (int i = 0; i < nodes; i++) {
      instances.add("localhost_" + i);
    }
    String[] resources = new String[] {
        "testResource1", "testResource2"
    };
    List<IdealState> idealStates = new ArrayList<IdealState>();
    for (int i = 0; i < resources.length; i++) {
      int partitions = 10;
      int replicas = 1;
      String resourceName = resources[i];
      ZNRecord record =
          DefaultTwoStateStrategy.calculateIdealState(instances, partitions, replicas,
              resourceName, "MASTER", "SLAVE");
      IdealState idealState = new IdealState(record);
      idealState.setStateModelDefId(StateModelDefId.from("MasterSlave"));

      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();
      accessor.setProperty(keyBuilder.idealStates(resourceName), idealState);

      idealStates.add(idealState);
    }
    // ADD A LIVE INSTANCE WITH A CURRENT STATE THAT CONTAINS RESOURCE WHICH NO
    // LONGER EXISTS IN IDEALSTATE
    String instanceName = "localhost_" + 3;
    LiveInstance liveInstance = new LiveInstance(instanceName);
    String sessionId = UUID.randomUUID().toString();
    liveInstance.setSessionId(sessionId);

    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    instanceConfig.setHostName("localhost");
    instanceConfig.setPort("3");

    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.liveInstance(instanceName), liveInstance);
    accessor.setProperty(keyBuilder.instanceConfig(instanceName), instanceConfig);

    String oldResource = "testResourceOld";
    CurrentState currentState = new CurrentState(oldResource);
    currentState.setState(PartitionId.from("testResourceOld_0"), State.from("OFFLINE"));
    currentState.setState(PartitionId.from("testResourceOld_1"), State.from("SLAVE"));
    currentState.setState(PartitionId.from("testResourceOld_2"), State.from("MASTER"));
    currentState.setStateModelDefRef("MasterSlave");
    accessor.setProperty(keyBuilder.currentState(instanceName, sessionId, oldResource),
        currentState);

    ResourceComputationStage stage = new ResourceComputationStage();
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);

    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    // +1 because it will have one for current state
    AssertJUnit.assertEquals(resources.length + 1, resourceMap.size());

    for (int i = 0; i < resources.length; i++) {
      String resourceName = resources[i];
      ResourceId resourceId = ResourceId.from(resourceName);
      IdealState idealState = idealStates.get(i);
      AssertJUnit.assertTrue(resourceMap.containsKey(resourceId));
      AssertJUnit.assertEquals(resourceMap.get(resourceId).getId(), resourceId);
      AssertJUnit.assertEquals(resourceMap.get(resourceId).getIdealState().getStateModelDefId(),
          idealState.getStateModelDefId());
      AssertJUnit.assertEquals(resourceMap.get(resourceId).getSubUnitSet().size(),
          idealState.getNumPartitions());
    }
    // Test the data derived from CurrentState
    ResourceId oldResourceId = ResourceId.from(oldResource);
    AssertJUnit.assertTrue(resourceMap.containsKey(oldResourceId));
    AssertJUnit.assertEquals(resourceMap.get(oldResourceId).getId(), oldResourceId);
    AssertJUnit.assertEquals(resourceMap.get(oldResourceId).getIdealState().getStateModelDefId(),
        currentState.getStateModelDefId());
    AssertJUnit.assertEquals(resourceMap.get(oldResourceId).getSubUnitSet().size(), currentState
        .getTypedPartitionStateMap().size());

  }

  @Test
  public void testNull() {
    ClusterEvent event = new ClusterEvent("sampleEvent");
    ResourceComputationStage stage = new ResourceComputationStage();
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    boolean exceptionCaught = false;
    try {
      stage.process(event);
    } catch (Exception e) {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    stage.postProcess();
  }

  // public void testEmptyCluster()
  // {
  // ClusterEvent event = new ClusterEvent("sampleEvent");
  // ClusterManager manager = new Mocks.MockManager();
  // event.addAttribute("clustermanager", manager);
  // ResourceComputationStage stage = new ResourceComputationStage();
  // StageContext context = new StageContext();
  // stage.init(context);
  // stage.preProcess();
  // boolean exceptionCaught = false;
  // try
  // {
  // stage.process(event);
  // } catch (Exception e)
  // {
  // exceptionCaught = true;
  // }
  // Assert.assertTrue(exceptionCaught);
  // stage.postProcess();
  // }

}
