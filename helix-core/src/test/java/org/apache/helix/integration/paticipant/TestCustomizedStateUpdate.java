package org.apache.helix.integration.paticipant;

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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.customizedstate.CustomizedStateProvider;
import org.apache.helix.customizedstate.CustomizedStateProviderFactory;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.model.CustomizedState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCustomizedStateUpdate extends ZkStandAloneCMTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestCustomizedStateUpdate.class);
  private final String CUSTOMIZE_STATE_NAME = "testState1";
  private final String PARTITION_NAME1 = "testPartition1";
  private final String PARTITION_NAME2 = "testPartition2";
  private final String RESOURCE_NAME = "testResource1";

  @Test
  public void testUpdateCustomizedState() throws Exception {
    HelixManager manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    manager.connect();
    _participants[0].connect();

    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    PropertyKey propertyKey = dataAccessor.keyBuilder()
        .customizedStates(_participants[0].getInstanceName(), CUSTOMIZE_STATE_NAME);
    CustomizedState customizedStates = manager.getHelixDataAccessor().getProperty(propertyKey);
    Assert.assertNull(customizedStates);

    CustomizedStateProvider mockProvider = CustomizedStateProviderFactory.getInstance()
        .buildCustomizedStateProvider(manager, _participants[0].getInstanceName());

    // test adding customized state for a partition
    Map<String, String> customizedStateMap = new HashMap<>();
    customizedStateMap.put("PREVIOUS_STATE", "STARTED");
    customizedStateMap.put("CURRENT_STATE", "END_OF_PUSH_RECEIVED");
    mockProvider.updateCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1,
        customizedStateMap);

    CustomizedState customizedState =
        mockProvider.getCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    Assert.assertNotNull(customizedState);
    Assert.assertEquals(customizedState.getId(), RESOURCE_NAME);
    Map<String, Map<String, String>> mapView = customizedState.getRecord().getMapFields();
    Assert.assertEquals(mapView.keySet().size(), 1);
    Assert.assertEquals(mapView.keySet().iterator().next(), PARTITION_NAME1);
    Assert.assertEquals(mapView.get(PARTITION_NAME1).keySet().size(), 2);
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("PREVIOUS_STATE"), "STARTED");
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("CURRENT_STATE"), "END_OF_PUSH_RECEIVED");

    // test update customized state for previous partition
    Map<String, String> stateMap1 = new HashMap<>();
    stateMap1.put("PREVIOUS_STATE", "END_OF_PUSH_RECEIVED");
    stateMap1.put("CURRENT_STATE", "COMPLETED");
    mockProvider.updateCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1,
        stateMap1);

    customizedState = mockProvider.getCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    Assert.assertNotNull(customizedState);
    Assert.assertEquals(customizedState.getId(), RESOURCE_NAME);
    mapView = customizedState.getRecord().getMapFields();
    Assert.assertEquals(mapView.keySet().size(), 1);
    Assert.assertEquals(mapView.keySet().iterator().next(), PARTITION_NAME1);
    Assert.assertEquals(mapView.get(PARTITION_NAME1).keySet().size(), 2);
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("PREVIOUS_STATE"), "END_OF_PUSH_RECEIVED");
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("CURRENT_STATE"), "COMPLETED");

    // test adding adding customized state for a new partition in the same resource
    Map<String, String> stateMap2 = new HashMap<>();
    stateMap2.put("PREVIOUS_STATE", "STARTED");
    stateMap2.put("CURRENT_STATE", "END_OF_PUSH_RECEIVED");
    mockProvider.updateCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME2,
        stateMap2);

    customizedState = mockProvider.getCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    Assert.assertNotNull(customizedState);
    Assert.assertEquals(customizedState.getId(), RESOURCE_NAME);
    mapView = customizedState.getRecord().getMapFields();
    Assert.assertEquals(mapView.keySet().size(), 2);
    Assert.assertEqualsNoOrder(mapView.keySet().toArray(), new String[] {
        PARTITION_NAME1, PARTITION_NAME2
    });

    Map<String, String> partitionMap1 = mockProvider
        .getPerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1);
    Assert.assertEquals(partitionMap1.keySet().size(), 2);
    Assert.assertEquals(partitionMap1.get("PREVIOUS_STATE"), "END_OF_PUSH_RECEIVED");
    Assert.assertEquals(partitionMap1.get("CURRENT_STATE"), "COMPLETED");

    Map<String, String> partitionMap2 = mockProvider
        .getPerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME2);
    Assert.assertEquals(partitionMap2.keySet().size(), 2);
    Assert.assertEquals(partitionMap2.get("PREVIOUS_STATE"), "STARTED");
    Assert.assertEquals(partitionMap2.get("CURRENT_STATE"), "END_OF_PUSH_RECEIVED");

    // test delete
    mockProvider.deletePerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1);
    customizedState = mockProvider.getCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    Assert.assertNotNull(customizedState);
    Assert.assertEquals(customizedState.getId(), RESOURCE_NAME);
    mapView = customizedState.getRecord().getMapFields();
    Assert.assertEquals(mapView.keySet().size(), 1);
    Assert.assertEquals(mapView.keySet().iterator().next(), PARTITION_NAME2);

    manager.disconnect();
  }
}
