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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.customizedstate.CustomizedStateProvider;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.model.CustomizedState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static java.lang.Thread.*;


public class TestCustomizedStateUpdate extends ZkStandAloneCMTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestCustomizedStateUpdate.class);
  private final String CUSTOMIZE_STATE_NAME = "testState1";
  private final String PARTITION_NAME1 = "testPartition1";
  private final String PARTITION_NAME2 = "testPartition2";
  private final String RESOURCE_NAME = "testResource1";

  @Test
  public void testUpdateCustomizedState() throws Exception {
    HelixManager manager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    manager.connect();
    _participants[0].connect();

    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    PropertyKey propertyKey =
        dataAccessor.keyBuilder().customizedStates(_participants[0].getInstanceName(), CUSTOMIZE_STATE_NAME);
    CustomizedState customizedStates = manager.getHelixDataAccessor().getProperty(propertyKey);
    Assert.assertNull(customizedStates);

    CustomizedStateProvider mockProvider = new CustomizedStateProvider(manager, _participants[0].getInstanceName());

    // test single update
    Map<String, String> stateMap = new HashMap<>();
    stateMap.put("PREVIOUS_STATE", "STARTED");
    stateMap.put("CURRENT_STATE", "END_OF_PUSH_RECEIVED");
    Map<String, Map<String, String>> partitionMap = new HashMap<>();
    partitionMap.put(PARTITION_NAME1, stateMap);
    Map<String, Map<String, Map<String, String>>> resourceMap = new HashMap<>();
    resourceMap.put(RESOURCE_NAME, partitionMap);
    mockProvider.updateCustomizedState(CUSTOMIZE_STATE_NAME, resourceMap);

    PropertyKey newPropertyKey = dataAccessor.keyBuilder()
        .customizedState(_participants[0].getInstanceName(), CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    CustomizedState customizedState = manager.getHelixDataAccessor().getProperty(newPropertyKey);
    Assert.assertNotNull(customizedState);
    Assert.assertEquals(customizedState.getId(), RESOURCE_NAME);
    Map<String, Map<String, String>> mapView = customizedState.getRecord().getMapFields();
    Assert.assertEquals(mapView.keySet().size(), 1);
    Assert.assertEquals(mapView.keySet().iterator().next(), PARTITION_NAME1);
    Assert.assertEquals(mapView.get(PARTITION_NAME1).keySet().size(), 2);
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("PREVIOUS_STATE"), "STARTED");
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("CURRENT_STATE"), "END_OF_PUSH_RECEIVED");

    //test batch update
    Map<String, String> stateMap1 = new HashMap<>();
    stateMap1.put("PREVIOUS_STATE", "END_OF_PUSH_RECEIVED");
    stateMap1.put("CURRENT_STATE", "COMPLETED");
    Map<String, Map<String, String>> partitionMap1 = new HashMap<>();
    partitionMap1.put(PARTITION_NAME1, stateMap1);

    Map<String, String> stateMap2 = new HashMap<>();
    stateMap2.put("PREVIOUS_STATE", "STARTED");
    stateMap2.put("CURRENT_STATE", "END_OF_PUSH_RECEIVED");
    partitionMap1.put(PARTITION_NAME2, stateMap2);

    Map<String, Map<String, Map<String, String>>> resourceMap1 = new HashMap<>();
    resourceMap1.put(RESOURCE_NAME, partitionMap1);
    mockProvider.updateCustomizedState(CUSTOMIZE_STATE_NAME, resourceMap1);


    customizedState = manager.getHelixDataAccessor().getProperty(newPropertyKey);
    mapView = customizedState.getRecord().getMapFields();
    Assert.assertEquals(mapView.keySet().size(), 2);
    Set<String> keys = new HashSet<>();
    keys.add(PARTITION_NAME1);
    keys.add(PARTITION_NAME2);
    Assert.assertEquals(mapView.keySet(), keys);
    Assert.assertEquals(mapView.get(PARTITION_NAME1).keySet().size(), 2);
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("PREVIOUS_STATE"), "END_OF_PUSH_RECEIVED");
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("CURRENT_STATE"), "COMPLETED");

    manager.disconnect();
  }
}
