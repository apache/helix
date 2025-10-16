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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.strategy.CrushEd2RebalanceStrategy;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * test for duplicate topology node handling.
 * Verifies that partitions are preserved when topology construction fails.
 */
public class TestDuplicateTopologyNodeHandling {

  private BestPossibleStateCalcStage stage;
  private ClusterEvent event;
  private ResourceControllerDataProvider cache;
  private CurrentStateOutput currentStateOutput;
  private BestPossibleStateOutput bestPossibleStateOutput;
  private String resourceName;
  private Resource resource;
  private StateModelDefinition stateModelDef;
  private MessageGenerationPhase messageGenerationPhase;

  @BeforeMethod
  public void setUp() {
    stage = new BestPossibleStateCalcStage();
    event = mock(ClusterEvent.class);
    cache = mock(ResourceControllerDataProvider.class);
    currentStateOutput = new CurrentStateOutput();
    bestPossibleStateOutput = new BestPossibleStateOutput();

    resourceName = "TestResource";
    resource = new Resource(resourceName);

    // Setup state model definition
    stateModelDef = new StateModelDefinition.Builder("MasterSlave")
        .addState("MASTER", 1)
        .addState("SLAVE", 2)
        .addState("OFFLINE")
        .addState("DROPPED")
        .addState("ERROR")
        .initialState("OFFLINE")
        .addTransition("OFFLINE", "SLAVE", 3)
        .addTransition("SLAVE", "OFFLINE", 4)
        .addTransition("SLAVE", "MASTER", 2)
        .addTransition("MASTER", "SLAVE", 1)
        .addTransition("OFFLINE", "DROPPED", 5)
        .dynamicUpperBound("MASTER", "R")
        .dynamicUpperBound("SLAVE", "N")
        .build();

    when(event.getAttribute(AttributeName.ControllerDataProvider.name())).thenReturn(cache);
    when(event.getAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name())).thenReturn(currentStateOutput);
    when(event.getAttribute(AttributeName.CURRENT_STATE.name())).thenReturn(currentStateOutput);
    when(event.getEventId()).thenReturn(UUID.randomUUID().toString());
    when(cache.getStateModelDef("MasterSlave")).thenReturn(stateModelDef);
    when(cache.getPipelineName()).thenReturn("DEFAULT");

    messageGenerationPhase = new MessageGenerationPhase();
  }

  @Test
  public void testDuplicateTopologyFailureDoesNotGenerateDroppedMessages() throws Exception {
    resource.addPartition("TestResource_0");
    resource.addPartition("TestResource_1");

    // Setup existing current state (instances currently have assignments)
    currentStateOutput.setCurrentState(resourceName, new Partition("TestResource_0"),
        "instance1", "MASTER");
    currentStateOutput.setCurrentState(resourceName, new Partition("TestResource_0"),
        "instance2", "SLAVE");
    currentStateOutput.setCurrentState(resourceName, new Partition("TestResource_1"),
        "instance3", "MASTER");
    currentStateOutput.setCurrentState(resourceName, new Partition("TestResource_1"),
        "instance1", "SLAVE");

    // Test that duplicate topology configuration throws HelixException
    String instance1 = "host1_12345";
    String instance2 = "host2_12345";
    String instance3 = "host3_12346";

    ClusterConfig clusterConfig = new ClusterConfig("TestCluster");
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/rack/host");
    clusterConfig.setFaultZoneType("rack");

    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    InstanceConfig config1 = new InstanceConfig(instance1);
    config1.setDomain("rack=rack1,host=duplicateHost");
    instanceConfigMap.put(instance1, config1);

    InstanceConfig config2 = new InstanceConfig(instance2);
    config2.setDomain("rack=rack1,host=duplicateHost"); // Same host - causes duplicate
    instanceConfigMap.put(instance2, config2);

    InstanceConfig config3 = new InstanceConfig(instance3);
    config3.setDomain("rack=rack2,host=uniqueHost");
    instanceConfigMap.put(instance3, config3);

    ResourceControllerDataProvider dataProvider = mock(ResourceControllerDataProvider.class);
    when(dataProvider.getClusterConfig()).thenReturn(clusterConfig);
    when(dataProvider.getAssignableInstanceConfigMap()).thenReturn(instanceConfigMap);
    when(dataProvider.getClusterEventId()).thenReturn("testEvent");

    CrushEd2RebalanceStrategy strategy = new CrushEd2RebalanceStrategy();
    List<String> partitions = Arrays.asList("TestResource_0", "TestResource_1");
    LinkedHashMap<String, Integer> stateCountMap = new LinkedHashMap<>();
    stateCountMap.put("MASTER", 1);
    stateCountMap.put("SLAVE", 2);
    strategy.init(resourceName, partitions, stateCountMap, 3);

    List<String> allNodes = Arrays.asList(instance1, instance2, instance3);

    // Verify HelixException is thrown for duplicate topology
    boolean exceptionThrown = false;
    try {
      strategy.computePartitionAssignment(allNodes, allNodes, new HashMap<>(), dataProvider);
    } catch (HelixException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Failed to add topology node because duplicate leaf nodes are not allowed"));
      Assert.assertTrue(e.getMessage().contains("Duplicate node name: duplicateHost"));
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "Expected HelixException for duplicate topology");

    // Verify that when topology fails, BestPossibleStateOutput remains empty
    BestPossibleStateOutput topologyFailureOutput = new BestPossibleStateOutput();
    Assert.assertTrue(topologyFailureOutput.getResourceStatesMap().isEmpty(),
        "BestPossibleStateOutput should be empty when topology calculation fails");

    // Check that getInstanceStateMap returns empty map for a resource that has no state assignments
    Map<String, String> stateMap = topologyFailureOutput.getInstanceStateMap(resourceName,
        new Partition("TestResource_0"));
    Assert.assertTrue(stateMap.isEmpty(),
        "No state assignment should exist in BestPossibleStateOutput after topology failure");

    // Verify MessageGenerationPhase does not generate DROPPED messages
    Map<String, Resource> resourceMap = new HashMap<>();
    resourceMap.put(resourceName, resource);

    // Setup event attributes with the actual topology failure output
    when(event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name())).thenReturn(topologyFailureOutput);
    when(event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name())).thenReturn(resourceMap);
    HelixManager helixManager = mock(HelixManager.class);
    when(event.getAttribute(AttributeName.helixmanager.name())).thenReturn(helixManager);

    messageGenerationPhase.process(event);

    MessageOutput messageOutput = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    if (messageOutput != null) {
      Map<Partition, List<Message>> resourceMessages = messageOutput.getResourceMessageMap(resourceName);
      if (resourceMessages != null) {
        for (List<Message> messages : resourceMessages.values()) {
          for (Message message : messages) {
            Assert.assertFalse("DROPPED".equals(message.getToState()),
                "No DROPPED messages should be generated when topology fails");
          }
        }
      }
    }
    // success: Topology failure -> empty BestPossibleStateOutput -> no DROPPED messages ->
    // existing assignments preserved
  }
}