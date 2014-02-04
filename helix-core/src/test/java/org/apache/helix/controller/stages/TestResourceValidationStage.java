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

import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.Mocks;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.ClusterConfiguration;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class TestResourceValidationStage {
  private static final String PARTICIPANT = "localhost_1234";
  private static final String STATE = "OFFLINE";

  @Test
  public void testIdealStateValidity() throws Exception {
    Mocks.MockAccessor accessor = new Mocks.MockAccessor();

    // create some ideal states
    ResourceId masterSlaveCustomResource = ResourceId.from("masterSlaveCustomResource");
    ResourceId onlineOfflineFullAutoResource = ResourceId.from("onlineOfflineFullAutoResource");
    ResourceId masterSlaveSemiAutoInvalidResource =
        ResourceId.from("masterSlaveSemiAutoInvalidResource");
    createIS(accessor, masterSlaveCustomResource, "MasterSlave", RebalanceMode.CUSTOMIZED);
    createIS(accessor, onlineOfflineFullAutoResource, "OnlineOffline", RebalanceMode.FULL_AUTO);
    createIS(accessor, masterSlaveSemiAutoInvalidResource, "MasterSlave", RebalanceMode.SEMI_AUTO);

    // create some ideal state specs
    createISSpec(accessor, masterSlaveCustomResource + "_spec", "MasterSlave",
        RebalanceMode.CUSTOMIZED);
    createISSpec(accessor, onlineOfflineFullAutoResource + "_spec", "OnlineOffline",
        RebalanceMode.FULL_AUTO);
    ClusterConfiguration clusterConfiguration =
        accessor.getProperty(accessor.keyBuilder().clusterConfig());

    // add some state models
    addStateModels(accessor);

    // refresh the cache
    ClusterEvent event = new ClusterEvent("testEvent");
    ClusterId clusterId = new ClusterId("sampleClusterId");
    ClusterAccessor clusterAccessor = new MockClusterAccessor(clusterId, accessor);
    Cluster cluster = clusterAccessor.readCluster();
    event.addAttribute("Cluster", cluster);
    event.addAttribute(AttributeName.IDEAL_STATE_RULES.toString(),
        clusterConfiguration.getIdealStateRules());

    // run resource computation
    new ResourceComputationStage().process(event);
    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    Assert.assertTrue(resourceMap.containsKey(masterSlaveCustomResource));
    Assert.assertTrue(resourceMap.containsKey(onlineOfflineFullAutoResource));
    Assert.assertTrue(resourceMap.containsKey(masterSlaveSemiAutoInvalidResource));

    // run resource validation
    new ResourceValidationStage().process(event);
    Map<ResourceId, ResourceConfig> finalResourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    Assert.assertTrue(finalResourceMap.containsKey(masterSlaveCustomResource));
    Assert.assertTrue(finalResourceMap.containsKey(onlineOfflineFullAutoResource));
    Assert.assertFalse(finalResourceMap.containsKey(masterSlaveSemiAutoInvalidResource));
  }

  @Test
  public void testNoSpec() throws Exception {
    Mocks.MockAccessor accessor = new Mocks.MockAccessor();

    // create an ideal state and no spec
    ResourceId masterSlaveCustomResource = ResourceId.from("masterSlaveCustomResource");
    createIS(accessor, masterSlaveCustomResource, "MasterSlave", RebalanceMode.CUSTOMIZED);

    // add some state models
    addStateModels(accessor);

    // refresh the cache
    ClusterEvent event = new ClusterEvent("testEvent");
    ClusterId clusterId = new ClusterId("sampleClusterId");
    ClusterAccessor clusterAccessor = new MockClusterAccessor(clusterId, accessor);
    Cluster cluster = clusterAccessor.readCluster();
    event.addAttribute("Cluster", cluster);
    Map<String, Map<String, String>> emptyMap = Maps.newHashMap();
    event.addAttribute(AttributeName.IDEAL_STATE_RULES.toString(), emptyMap);

    // run resource computation
    new ResourceComputationStage().process(event);
    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    Assert.assertTrue(resourceMap.containsKey(masterSlaveCustomResource));

    // run resource validation
    new ResourceValidationStage().process(event);
    Map<ResourceId, ResourceConfig> finalResourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    Assert.assertTrue(finalResourceMap.containsKey(masterSlaveCustomResource));
  }

  @Test
  public void testMissingStateModel() throws Exception {
    Mocks.MockAccessor accessor = new Mocks.MockAccessor();

    // create an ideal state and no spec
    ResourceId masterSlaveCustomResource = ResourceId.from("masterSlaveCustomResource");
    ResourceId leaderStandbyCustomResource = ResourceId.from("leaderStandbyCustomResource");
    createIS(accessor, masterSlaveCustomResource, "MasterSlave", RebalanceMode.CUSTOMIZED);
    createIS(accessor, leaderStandbyCustomResource, "LeaderStandby", RebalanceMode.CUSTOMIZED);

    // add some state models (but not leader standby)
    addStateModels(accessor);

    // refresh the cache
    ClusterEvent event = new ClusterEvent("testEvent");
    ClusterId clusterId = new ClusterId("sampleClusterId");
    ClusterAccessor clusterAccessor = new MockClusterAccessor(clusterId, accessor);
    Cluster cluster = clusterAccessor.readCluster();
    event.addAttribute("Cluster", cluster);
    Map<String, Map<String, String>> emptyMap = Maps.newHashMap();
    event.addAttribute(AttributeName.IDEAL_STATE_RULES.toString(), emptyMap);

    // run resource computation
    new ResourceComputationStage().process(event);
    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    Assert.assertTrue(resourceMap.containsKey(masterSlaveCustomResource));
    Assert.assertTrue(resourceMap.containsKey(leaderStandbyCustomResource));

    // run resource validation
    new ResourceValidationStage().process(event);
    Map<ResourceId, ResourceConfig> finalResourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    Assert.assertTrue(finalResourceMap.containsKey(masterSlaveCustomResource));
    Assert.assertFalse(finalResourceMap.containsKey(leaderStandbyCustomResource));
  }

  private void createIS(HelixDataAccessor accessor, ResourceId resourceId, String stateModelDefRef,
      RebalanceMode rebalanceMode) {
    IdealState idealState = new IdealState(resourceId);
    idealState.setRebalanceMode(rebalanceMode);
    idealState.setStateModelDefRef(stateModelDefRef);
    idealState.setNumPartitions(1);
    idealState.setReplicas("1");
    idealState.getRecord().setListField(resourceId + "_0", ImmutableList.of(PARTICIPANT));
    idealState.getRecord().setMapField(resourceId + "_0", ImmutableMap.of(PARTICIPANT, STATE));
    accessor.setProperty(accessor.keyBuilder().idealStates(resourceId.toString()), idealState);
  }

  private void createISSpec(HelixDataAccessor accessor, String specId, String stateModelDefRef,
      RebalanceMode rebalanceMode) {
    PropertyKey propertyKey = accessor.keyBuilder().clusterConfig();
    HelixProperty property = accessor.getProperty(propertyKey);
    if (property == null) {
      property = new HelixProperty("sampleClusterConfig");
    }
    String key = "IdealStateRule!" + specId;
    String value =
        IdealStateProperty.REBALANCE_MODE.toString() + "=" + rebalanceMode.toString() + ","
            + IdealStateProperty.STATE_MODEL_DEF_REF.toString() + "=" + stateModelDefRef;
    property.getRecord().setSimpleField(key, value);
    accessor.setProperty(propertyKey, property);
  }

  private void addStateModels(HelixDataAccessor accessor) {
    StateModelDefinition masterSlave =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
    accessor.setProperty(accessor.keyBuilder().stateModelDef(masterSlave.getId()), masterSlave);
    StateModelDefinition onlineOffline =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline());
    accessor.setProperty(accessor.keyBuilder().stateModelDef(onlineOffline.getId()), onlineOffline);
  }

  private static class MockClusterAccessor extends ClusterAccessor {
    public MockClusterAccessor(ClusterId clusterId, HelixDataAccessor accessor) {
      super(clusterId, accessor);
    }

    @Override
    public boolean isClusterStructureValid() {
      return true;
    }
  }
}
