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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.customizedstate.CustomizedStateProvider;
import org.apache.helix.customizedstate.CustomizedStateProviderFactory;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.model.CustomizedState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestCustomizedStateUpdate extends ZkStandAloneCMTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestCustomizedStateUpdate.class);
  private final String CUSTOMIZE_STATE_NAME = "testState1";
  private final String PARTITION_NAME1 = "testPartition1";
  private final String PARTITION_NAME2 = "testPartition2";
  private final String RESOURCE_NAME = "testResource1";
  private final String PARTITION_STATE = "partitionState";
  private static CustomizedStateProvider _mockProvider;
  private PropertyKey _propertyKey;
  private HelixDataAccessor _dataAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _participants[0].connect();
    _mockProvider = CustomizedStateProviderFactory.getInstance()
        .buildCustomizedStateProvider(_manager, _participants[0].getInstanceName());
    _dataAccessor = _manager.getHelixDataAccessor();
    _propertyKey = _dataAccessor.keyBuilder()
        .customizedStates(_participants[0].getInstanceName(), CUSTOMIZE_STATE_NAME);
  }

  @BeforeMethod
  public void beforeMethod() {
    _dataAccessor.removeProperty(_propertyKey);
    CustomizedState customizedStates = _dataAccessor.getProperty(_propertyKey);
    Assert.assertNull(customizedStates);
  }

  @Test
  public void testUpdateCustomizedState() {

    // test adding customized state for a partition
    Map<String, String> customizedStateMap = new HashMap<>();
    customizedStateMap.put("PREVIOUS_STATE", "STARTED");
    customizedStateMap.put("CURRENT_STATE", "END_OF_PUSH_RECEIVED");
    _mockProvider.updateCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1,
        customizedStateMap);

    CustomizedState customizedState =
        _mockProvider.getCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    Assert.assertNotNull(customizedState);
    Assert.assertEquals(customizedState.getId(), RESOURCE_NAME);
    Map<String, Map<String, String>> mapView = customizedState.getRecord().getMapFields();
    Assert.assertEquals(mapView.keySet().size(), 1);
    Assert.assertEquals(mapView.keySet().iterator().next(), PARTITION_NAME1);
    // Updated 2 fields + START_TIME field is automatically updated for monitoring
    Assert.assertEquals(mapView.get(PARTITION_NAME1).keySet().size(), 3);
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("PREVIOUS_STATE"), "STARTED");
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("CURRENT_STATE"), "END_OF_PUSH_RECEIVED");

    // test partial update customized state for previous partition
    Map<String, String> stateMap1 = new HashMap<>();
    stateMap1.put("PREVIOUS_STATE", "END_OF_PUSH_RECEIVED");
    _mockProvider
        .updateCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1, stateMap1);

    customizedState = _mockProvider.getCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    Assert.assertNotNull(customizedState);
    Assert.assertEquals(customizedState.getId(), RESOURCE_NAME);
    mapView = customizedState.getRecord().getMapFields();
    Assert.assertEquals(mapView.keySet().size(), 1);
    Assert.assertEquals(mapView.keySet().iterator().next(), PARTITION_NAME1);
    Assert.assertEquals(mapView.get(PARTITION_NAME1).keySet().size(), 3);
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("PREVIOUS_STATE"), "END_OF_PUSH_RECEIVED");
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("CURRENT_STATE"), "END_OF_PUSH_RECEIVED");

    // test full update customized state for previous partition
    stateMap1 = new HashMap<>();
    stateMap1.put("PREVIOUS_STATE", "END_OF_PUSH_RECEIVED");
    stateMap1.put("CURRENT_STATE", "COMPLETED");
    _mockProvider
        .updateCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1, stateMap1);

    customizedState = _mockProvider.getCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    Assert.assertNotNull(customizedState);
    Assert.assertEquals(customizedState.getId(), RESOURCE_NAME);
    mapView = customizedState.getRecord().getMapFields();
    Assert.assertEquals(mapView.keySet().size(), 1);
    Assert.assertEquals(mapView.keySet().iterator().next(), PARTITION_NAME1);
    Assert.assertEquals(mapView.get(PARTITION_NAME1).keySet().size(), 3);
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("PREVIOUS_STATE"), "END_OF_PUSH_RECEIVED");
    Assert.assertEquals(mapView.get(PARTITION_NAME1).get("CURRENT_STATE"), "COMPLETED");

    // test adding adding customized state for a new partition in the same resource
    Map<String, String> stateMap2 = new HashMap<>();
    stateMap2.put("PREVIOUS_STATE", "STARTED");
    stateMap2.put("CURRENT_STATE", "END_OF_PUSH_RECEIVED");
    _mockProvider
        .updateCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME2, stateMap2);

    customizedState = _mockProvider.getCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    Assert.assertNotNull(customizedState);
    Assert.assertEquals(customizedState.getId(), RESOURCE_NAME);
    mapView = customizedState.getRecord().getMapFields();
    Assert.assertEquals(mapView.keySet().size(), 2);
    Assert.assertEqualsNoOrder(mapView.keySet().toArray(),
        new String[] { PARTITION_NAME1, PARTITION_NAME2 });

    Map<String, String> partitionMap1 = _mockProvider
        .getPerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1);
    Assert.assertEquals(partitionMap1.keySet().size(), 3);
    Assert.assertEquals(partitionMap1.get("PREVIOUS_STATE"), "END_OF_PUSH_RECEIVED");
    Assert.assertEquals(partitionMap1.get("CURRENT_STATE"), "COMPLETED");

    Map<String, String> partitionMap2 = _mockProvider
        .getPerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME2);
    Assert.assertEquals(partitionMap2.keySet().size(), 3);
    Assert.assertEquals(partitionMap2.get("PREVIOUS_STATE"), "STARTED");
    Assert.assertEquals(partitionMap2.get("CURRENT_STATE"), "END_OF_PUSH_RECEIVED");

    // test delete customized state for a partition
    _mockProvider
        .deletePerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1);
    customizedState = _mockProvider.getCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    Assert.assertNotNull(customizedState);
    Assert.assertEquals(customizedState.getId(), RESOURCE_NAME);
    mapView = customizedState.getRecord().getMapFields();
    Assert.assertEquals(mapView.keySet().size(), 1);
    Assert.assertEquals(mapView.keySet().iterator().next(), PARTITION_NAME2);

    _mockProvider
        .deletePerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME2);
    customizedState = _mockProvider.getCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    Assert.assertNull(customizedState);
  }

  @Test
  public void testUpdateSinglePartitionCustomizedState() {
    _mockProvider.updateCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1,
        PARTITION_STATE);

    // get customized state
    CustomizedState customizedState =
        _mockProvider.getCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    // START_TIME field is automatically updated for monitoring
    Assert.assertEquals(
        customizedState.getPartitionStateMap(CustomizedState.CustomizedStateProperty.CURRENT_STATE)
            .size(), 1);
    Map<String, String> map = new HashMap<>();
    map.put(PARTITION_NAME1, null);
    Assert.assertEquals(customizedState
        .getPartitionStateMap(CustomizedState.CustomizedStateProperty.PREVIOUS_STATE), map);
    Assert.assertEquals(
        customizedState.getPartitionStateMap(CustomizedState.CustomizedStateProperty.START_TIME).size(),
        1);
    Assert.assertEquals(
        customizedState.getPartitionStateMap(CustomizedState.CustomizedStateProperty.END_TIME),
        map);
    Assert.assertEquals(customizedState.getState(PARTITION_NAME1), PARTITION_STATE);
    Assert.assertNull(customizedState.getState(PARTITION_NAME2));
    Assert.assertTrue(customizedState.isValid());

    // get per partition customized state
    map = new HashMap<>();
    map.put(CustomizedState.CustomizedStateProperty.CURRENT_STATE.name(), PARTITION_STATE);
    Map<String, String> partitionCustomizedState = _mockProvider
        .getPerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1);
    partitionCustomizedState.remove(CustomizedState.CustomizedStateProperty.START_TIME.name());
    Assert.assertEquals(partitionCustomizedState, map);
    Assert.assertNull(_mockProvider
        .getPerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME2));
  }

  @Test
  public void testUpdateSinglePartitionCustomizedStateWithNullField() {
    _mockProvider
        .updateCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1, (String) null);

    // get customized state
    CustomizedState customizedState =
        _mockProvider.getCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    Map<String, String> map = new HashMap<>();
    map.put(PARTITION_NAME1, null);
    Assert.assertEquals(
        customizedState.getPartitionStateMap(CustomizedState.CustomizedStateProperty.CURRENT_STATE),
        map);
    Assert.assertEquals(customizedState.getState(PARTITION_NAME1), null);
    Assert.assertTrue(customizedState.isValid());

    // get per partition customized state
    map = new HashMap<>();
    map.put(CustomizedState.CustomizedStateProperty.CURRENT_STATE.name(), null);
    Map<String, String> partitionCustomizedState = _mockProvider
        .getPerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1);
    partitionCustomizedState.remove(CustomizedState.CustomizedStateProperty.START_TIME.name());
    Assert.assertEquals(partitionCustomizedState, map);
    Assert.assertNull(_mockProvider
        .getPerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME2));
  }

  @Test
  public void testUpdateCustomizedStateWithEmptyMap() {
    _mockProvider.updateCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1,
        new HashMap<>());

    // get customized state
    CustomizedState customizedState =
        _mockProvider.getCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME);
    Assert.assertNull(customizedState.getState(PARTITION_NAME1));
    Map<String, String> partitionStateMap =
        customizedState.getPartitionStateMap(CustomizedState.CustomizedStateProperty.CURRENT_STATE);
    Assert.assertNotNull(partitionStateMap);
    Assert.assertTrue(partitionStateMap.containsKey(PARTITION_NAME1));
    Assert.assertNull(partitionStateMap.get(PARTITION_NAME1));
    Assert.assertNull(customizedState.getState(PARTITION_NAME1));
    Assert.assertFalse(partitionStateMap.containsKey(PARTITION_NAME2));
    Assert.assertTrue(customizedState.isValid());

    // get per partition customized state
    Map<String, String> partitionCustomizedState = _mockProvider
        .getPerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1);
    // START_TIME field is automatically updated for monitoring
    Assert.assertEquals(partitionCustomizedState.size(), 1);
    Assert.assertNull(_mockProvider
        .getPerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME2));
  }

  @Test
  public void testDeleteNonExistingPerPartitionCustomizedState() {
    _mockProvider.updateCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1,
        PARTITION_STATE);
    _mockProvider
        .deletePerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME2);
    Assert.assertNotNull(_mockProvider
        .getPerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME1));
    Assert.assertNull(_mockProvider
        .getPerPartitionCustomizedState(CUSTOMIZE_STATE_NAME, RESOURCE_NAME, PARTITION_NAME2));
  }

  @Test
  public void testSimultaneousUpdateCustomizedState() {
    List<Callable<Boolean>> threads = new ArrayList<>();
    int threadCount = 10;
    for (int i = 0; i < threadCount; i++) {
      threads.add(new TestSimultaneousUpdate());
    }
    Map<String, Boolean> resultMap = TestHelper.startThreadsConcurrently(threads, 1000);
    Assert.assertEquals(resultMap.size(), threadCount);
    Boolean[] results = new Boolean[threadCount];
    Arrays.fill(results, true);
    Assert.assertEqualsNoOrder(resultMap.values().toArray(), results);
  }

  private static class TestSimultaneousUpdate implements Callable<Boolean> {
    private Random rand = new Random();

    @Override
    public Boolean call() {
      String customizedStateName = "testState";
      String resourceName = "resource" + String.valueOf(rand.nextInt(10));
      String partitionName = "partition" + String.valueOf(rand.nextInt(10));
      String partitionState = "Updated";
      try {
        _mockProvider.updateCustomizedState(customizedStateName, resourceName, partitionName,
            partitionState);
      } catch (Exception e) {
        return false;
      }
      Map<String, String> states = _mockProvider
          .getPerPartitionCustomizedState(customizedStateName, resourceName, partitionName);
      if (states == null) {
        return false;
      }
      return states.get(CustomizedState.CustomizedStateProperty.CURRENT_STATE.name())
          .equals(partitionState);
    }
  }
}
