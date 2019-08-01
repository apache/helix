package org.apache.helix.controller.rebalancer.waged.model;

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

import org.apache.helix.HelixException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.when;

public class TestAssignableNode extends AbstractTestClusterModel {
  @BeforeClass
  public void initialize() {
    super.initialize();
  }

  @Test
  public void testNormalUsage() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    Set<AssignableReplica> assignmentSet = generateReplicas(testCache);

    // Test 1 - initialization

    Set<String> expectedAssignmentSet1 = new HashSet<>(_partitionNames.subList(0, 2));
    Set<String> expectedAssignmentSet2 = new HashSet<>(_partitionNames.subList(2, 4));
    Map<String, Set<String>> expectedAssignment = new HashMap<>();
    expectedAssignment.put("Resource1", expectedAssignmentSet1);
    expectedAssignment.put("Resource2", expectedAssignmentSet2);
    Map<String, Integer> expectedCapacityMap = new HashMap<>();
    expectedCapacityMap.put("item1", 4);
    expectedCapacityMap.put("item2", 8);
    expectedCapacityMap.put("item3", 30);


    AssignableNode assignableNode = new AssignableNode(testCache, _testInstanceId, assignmentSet);
    Assert.assertTrue(assignableNode.getCurrentAssignmentsMap().equals(expectedAssignment));
    Assert.assertEquals(assignableNode.getCurrentAssignmentCount(), 4);
    Assert.assertEquals(assignableNode.getHighestCapacityUtilization(), 16.0 / 20.0, 0.005);
    Assert.assertTrue(assignableNode.getMaxCapacity().equals(_capacityDataMap));
    Assert.assertEquals(assignableNode.getMaxPartition(), 5);
    Assert.assertEquals(assignableNode.getInstanceTags(), _testInstanceTags);
    Assert.assertEquals(assignableNode.getFaultZone(), _testFaultZoneId);
    Assert.assertTrue(assignableNode.getDisabledPartitionsMap().equals(_disabledPartitionsMap));
    Assert.assertTrue(assignableNode.getCurrentCapacity().equals(expectedCapacityMap));

    // Test 2 - reduce assignment
    AssignableReplica removingReplica =
        new AssignableReplica(testCache.getResourceConfig(_resourceNames.get(1)),
            _partitionNames.get(2), "MASTER", 1);
    expectedAssignment.get(_resourceNames.get(1)).remove(_partitionNames.get(2));
    expectedCapacityMap.put("item1", 9);
    expectedCapacityMap.put("item2", 18);

    assignableNode.release(removingReplica);

    Assert.assertTrue(assignableNode.getCurrentAssignmentsMap().equals(expectedAssignment));
    Assert.assertEquals(assignableNode.getCurrentAssignmentCount(), 3);
    Assert.assertEquals(assignableNode.getHighestCapacityUtilization(), 11.0 / 20.0, 0.005);
    Assert.assertTrue(assignableNode.getMaxCapacity().equals(_capacityDataMap));
    Assert.assertEquals(assignableNode.getMaxPartition(), 5);
    Assert.assertEquals(assignableNode.getInstanceTags(), _testInstanceTags);
    Assert.assertEquals(assignableNode.getFaultZone(), _testFaultZoneId);
    Assert.assertTrue(assignableNode.getDisabledPartitionsMap().equals(_disabledPartitionsMap));
    Assert.assertTrue(assignableNode.getCurrentCapacity().equals(expectedCapacityMap));

    // Test 3 - add assignment
    AssignableReplica addingReplica =
        new AssignableReplica(testCache.getResourceConfig(_resourceNames.get(1)),
            _partitionNames.get(2), "SLAVE", 2);
    expectedAssignment.get(_resourceNames.get(1)).add(_partitionNames.get(2));
    expectedCapacityMap.put("item1", 4);
    expectedCapacityMap.put("item2", 8);

    assignableNode.assign(addingReplica);

    Assert.assertTrue(assignableNode.getCurrentAssignmentsMap().equals(expectedAssignment));
    Assert.assertEquals(assignableNode.getCurrentAssignmentCount(), 4);
    Assert.assertEquals(assignableNode.getHighestCapacityUtilization(), 16.0 / 20.0, 0.005);
    Assert.assertTrue(assignableNode.getMaxCapacity().equals(_capacityDataMap));
    Assert.assertEquals(assignableNode.getMaxPartition(), 5);
    Assert.assertEquals(assignableNode.getInstanceTags(), _testInstanceTags);
    Assert.assertEquals(assignableNode.getFaultZone(), _testFaultZoneId);
    Assert.assertTrue(assignableNode.getDisabledPartitionsMap().equals(_disabledPartitionsMap));
    Assert.assertTrue(assignableNode.getCurrentCapacity().equals(expectedCapacityMap));
  }

  @Test
  public void testReleaseNoPartition() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();

    AssignableNode assignableNode = new AssignableNode(testCache, _testInstanceId, Collections.emptyList());
    AssignableReplica removingReplica =
        new AssignableReplica(testCache.getResourceConfig(_resourceNames.get(1)),
            _partitionNames.get(2) + "non-exist", "MASTER", 1);

    // Release shall pass.
    assignableNode.release(removingReplica);
  }

  @Test(expectedExceptions = HelixException.class, expectedExceptionsMessageRegExp = "Resource Resource1 already has a replica from partition Partition1 on this node")
  public void testAssignAlreadyExist() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    Set<AssignableReplica> assignmentSet = generateReplicas(testCache);

    AssignableNode assignableNode = new AssignableNode(testCache, _testInstanceId, assignmentSet);
    AssignableReplica duplicateReplica =
        new AssignableReplica(testCache.getResourceConfig(_resourceNames.get(0)),
            _partitionNames.get(0), "SLAVE", 2);
    assignableNode.assign(duplicateReplica);
  }

  @Test(expectedExceptions = HelixException.class, expectedExceptionsMessageRegExp = "The domain configuration of instance testInstanceId is not complete. Type DOES_NOT_EXIST is not found.")
  public void testComputeFaultZoneNotFound() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();

    ClusterConfig testClusterConfig = new ClusterConfig("testClusterConfigId");
    testClusterConfig.setFaultZoneType("DOES_NOT_EXIST");
    testClusterConfig.setTopologyAwareEnabled(true);
    testClusterConfig.setTopology("/DOES_NOT_EXIST/");
    when(testCache.getClusterConfig()).thenReturn(testClusterConfig);

    InstanceConfig testInstanceConfig = new InstanceConfig("testInstanceConfigId");
    testInstanceConfig.setDomain("zone=2, instance=testInstance");
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    instanceConfigMap.put(_testInstanceId, testInstanceConfig);
    when(testCache.getInstanceConfigMap()).thenReturn(instanceConfigMap);

    new AssignableNode(testCache, _testInstanceId, Collections.emptyList());
  }

  @Test
  public void testComputeFaultZone() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();

    ClusterConfig testClusterConfig = new ClusterConfig("testClusterConfigId");
    testClusterConfig.setFaultZoneType("zone");
    testClusterConfig.setTopologyAwareEnabled(true);
    testClusterConfig.setTopology("/zone/instance");
    when(testCache.getClusterConfig()).thenReturn(testClusterConfig);

    InstanceConfig testInstanceConfig = new InstanceConfig("testInstanceConfigId");
    testInstanceConfig.setDomain("zone=2, instance=testInstance");
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    instanceConfigMap.put(_testInstanceId, testInstanceConfig);
    when(testCache.getInstanceConfigMap()).thenReturn(instanceConfigMap);

    AssignableNode assignableNode = new AssignableNode(testCache, _testInstanceId, Collections.emptyList());

    Assert.assertEquals(assignableNode.getFaultZone(), "2/");

    testClusterConfig = new ClusterConfig("testClusterConfigId");
    testClusterConfig.setFaultZoneType("instance");
    testClusterConfig.setTopologyAwareEnabled(true);
    testClusterConfig.setTopology("/zone/instance");
    when(testCache.getClusterConfig()).thenReturn(testClusterConfig);

    testInstanceConfig = new InstanceConfig("testInstanceConfigId");
    testInstanceConfig.setDomain("zone=2, instance=testInstance");
    instanceConfigMap = new HashMap<>();
    instanceConfigMap.put(_testInstanceId, testInstanceConfig);
    when(testCache.getInstanceConfigMap()).thenReturn(instanceConfigMap);

    assignableNode = new AssignableNode(testCache, _testInstanceId, Collections.emptyList());

    Assert.assertEquals(assignableNode.getFaultZone(), "2/testInstance/");
  }
}
