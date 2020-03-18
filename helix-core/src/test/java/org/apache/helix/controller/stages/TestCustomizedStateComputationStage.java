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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.helix.PropertyKey;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.CustomizedState;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;


public class TestCustomizedStateComputationStage extends BaseStageTest {
  private final String RESOURCE_NAME = "testResourceName";
  private final String PARTITION_NAME = "testResourceName_0";
  private final String CUSTOMIZED_STATE_NAME = "customizedState1";
  private final String INSTANCE_NAME = "localhost_1";

  @Test
  public void testEmptyCustomizedState() {
    Map<String, Resource> resourceMap = getResourceMap();
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider(_clusterName));
    CustomizedStateComputationStage stage = new CustomizedStateComputationStage();
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CustomizedStateOutput output = event.getAttribute(AttributeName.CUSTOMIZED_STATE.name());
    AssertJUnit.assertEquals(output
        .getPartitionCustomizedStateMap(CUSTOMIZED_STATE_NAME, RESOURCE_NAME,
            new Partition("testResourceName_0")).size(), 0);
  }

  @Test
  public void testSimpleCustomizedState() {
    // setup resource
    Map<String, Resource> resourceMap = getResourceMap();

    setupLiveInstances(5);

    // Add CustomizedStateAggregation Config
    CustomizedStateConfig config = new CustomizedStateConfig();
    List<String> aggregationEnabledTypes = new ArrayList<>();
    aggregationEnabledTypes.add(CUSTOMIZED_STATE_NAME);

    config.setAggregationEnabledTypes(aggregationEnabledTypes);

    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.customizedStateConfig(), config);

    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider(_clusterName));
    CustomizedStateComputationStage stage = new CustomizedStateComputationStage();
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CustomizedStateOutput output1 = event.getAttribute(AttributeName.CUSTOMIZED_STATE.name());
    AssertJUnit.assertEquals(output1
        .getPartitionCustomizedStateMap(CUSTOMIZED_STATE_NAME, RESOURCE_NAME,
            new Partition(PARTITION_NAME)).size(), 0);

    // Add a customized state
    CustomizedState customizedState = new CustomizedState(RESOURCE_NAME);
    customizedState.setState(PARTITION_NAME, "STARTED");
    accessor.setProperty(
        keyBuilder.customizedState(INSTANCE_NAME, CUSTOMIZED_STATE_NAME, RESOURCE_NAME),
        customizedState);

    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CustomizedStateOutput output2 = event.getAttribute(AttributeName.CUSTOMIZED_STATE.name());
    Partition partition = new Partition(PARTITION_NAME);
    AssertJUnit.assertEquals(output2
        .getPartitionCustomizedState(CUSTOMIZED_STATE_NAME, RESOURCE_NAME, partition,
            INSTANCE_NAME), "STARTED");
  }
}