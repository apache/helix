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
import org.apache.helix.MockAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestResourceValidationStage {
  private static final String PARTICIPANT = "localhost_1234";
  private static final String STATE = "OFFLINE";

  @Test
  public void testIdealStateValidity() throws Exception {
    MockAccessor accessor = new MockAccessor();

    // create some ideal states
    String masterSlaveCustomResource = "masterSlaveCustomResource";
    String onlineOfflineFullAutoResource = "onlineOfflineFullAutoResource";
    String masterSlaveSemiAutoInvalidResource = "masterSlaveSemiAutoInvalidResource";
    createIS(accessor, masterSlaveCustomResource, "MasterSlave", RebalanceMode.CUSTOMIZED);
    createIS(accessor, onlineOfflineFullAutoResource, "OnlineOffline", RebalanceMode.FULL_AUTO);
    createIS(accessor, masterSlaveSemiAutoInvalidResource, "MasterSlave", RebalanceMode.SEMI_AUTO);

    // create some ideal state specs
    createISSpec(accessor, masterSlaveCustomResource + "_spec", "MasterSlave",
        RebalanceMode.CUSTOMIZED);
    createISSpec(accessor, onlineOfflineFullAutoResource + "_spec", "OnlineOffline",
        RebalanceMode.FULL_AUTO);

    // add some state models
    addStateModels(accessor);

    // refresh the cache
    ClusterEvent event = new ClusterEvent("testEvent");
    ClusterDataCache cache = new ClusterDataCache();
    cache.refresh(accessor);
    event.addAttribute("ClusterDataCache", cache);

    // run resource computation
    new ResourceComputationStage().process(event);
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    Assert.assertTrue(resourceMap.containsKey(masterSlaveCustomResource));
    Assert.assertTrue(resourceMap.containsKey(onlineOfflineFullAutoResource));
    Assert.assertTrue(resourceMap.containsKey(masterSlaveSemiAutoInvalidResource));

    // run resource validation
    new ResourceValidationStage().process(event);
    Map<String, Resource> finalResourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    Assert.assertTrue(finalResourceMap.containsKey(masterSlaveCustomResource));
    Assert.assertTrue(finalResourceMap.containsKey(onlineOfflineFullAutoResource));
    Assert.assertFalse(finalResourceMap.containsKey(masterSlaveSemiAutoInvalidResource));
  }

  @Test
  public void testNoSpec() throws Exception {
    MockAccessor accessor = new MockAccessor();

    // create an ideal state and no spec
    String masterSlaveCustomResource = "masterSlaveCustomResource";
    createIS(accessor, masterSlaveCustomResource, "MasterSlave", RebalanceMode.CUSTOMIZED);

    // add some state models
    addStateModels(accessor);

    // refresh the cache
    ClusterEvent event = new ClusterEvent("testEvent");
    ClusterDataCache cache = new ClusterDataCache();
    cache.refresh(accessor);
    event.addAttribute("ClusterDataCache", cache);

    // run resource computation
    new ResourceComputationStage().process(event);
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    Assert.assertTrue(resourceMap.containsKey(masterSlaveCustomResource));

    // run resource validation
    new ResourceValidationStage().process(event);
    Map<String, Resource> finalResourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    Assert.assertTrue(finalResourceMap.containsKey(masterSlaveCustomResource));
  }

  @Test
  public void testMissingStateModel() throws Exception {
    MockAccessor accessor = new MockAccessor();

    // create an ideal state and no spec
    String masterSlaveCustomResource = "masterSlaveCustomResource";
    String leaderStandbyCustomResource = "leaderStandbyCustomResource";
    createIS(accessor, masterSlaveCustomResource, "MasterSlave", RebalanceMode.CUSTOMIZED);
    createIS(accessor, leaderStandbyCustomResource, "LeaderStandby", RebalanceMode.CUSTOMIZED);

    // add some state models (but not leader standby)
    addStateModels(accessor);

    // refresh the cache
    ClusterEvent event = new ClusterEvent("testEvent");
    ClusterDataCache cache = new ClusterDataCache();
    cache.refresh(accessor);
    event.addAttribute("ClusterDataCache", cache);

    // run resource computation
    new ResourceComputationStage().process(event);
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    Assert.assertTrue(resourceMap.containsKey(masterSlaveCustomResource));
    Assert.assertTrue(resourceMap.containsKey(leaderStandbyCustomResource));

    // run resource validation
    new ResourceValidationStage().process(event);
    Map<String, Resource> finalResourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    Assert.assertTrue(finalResourceMap.containsKey(masterSlaveCustomResource));
    Assert.assertFalse(finalResourceMap.containsKey(leaderStandbyCustomResource));
  }

  private void createIS(HelixDataAccessor accessor, String resourceId, String stateModelDefRef,
      RebalanceMode rebalanceMode) {
    IdealState idealState = new IdealState(resourceId);
    idealState.setRebalanceMode(rebalanceMode);
    idealState.setStateModelDefRef(stateModelDefRef);
    idealState.setNumPartitions(1);
    idealState.setReplicas("1");
    idealState.getRecord().setListField(resourceId + "_0", ImmutableList.of(PARTICIPANT));
    idealState.getRecord().setMapField(resourceId + "_0", ImmutableMap.of(PARTICIPANT, STATE));
    accessor.setProperty(accessor.keyBuilder().idealStates(resourceId), idealState);
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
}
