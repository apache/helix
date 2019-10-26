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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;

public class TestAssignableNode extends AbstractTestClusterModel {
  @BeforeClass
  public void initialize() {
    super.initialize();
  }

  @Test
  public void testNormalUsage() throws IOException {
    // Test 1 - initialize based on the data cache and check with the expected result
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    Set<AssignableReplica> assignmentSet = generateReplicas(testCache);

    Set<String> expectedTopStateAssignmentSet1 = new HashSet<>(_partitionNames.subList(0, 1));
    Set<String> expectedTopStateAssignmentSet2 = new HashSet<>(_partitionNames.subList(2, 3));
    Set<String> expectedAssignmentSet1 = new HashSet<>(_partitionNames.subList(0, 2));
    Set<String> expectedAssignmentSet2 = new HashSet<>(_partitionNames.subList(2, 4));
    Map<String, Set<String>> expectedAssignment = new HashMap<>();
    expectedAssignment.put("Resource1", expectedAssignmentSet1);
    expectedAssignment.put("Resource2", expectedAssignmentSet2);
    Map<String, Integer> expectedCapacityMap = new HashMap<>();
    expectedCapacityMap.put("item1", 4);
    expectedCapacityMap.put("item2", 8);
    expectedCapacityMap.put("item3", 30);

    AssignableNode assignableNode = new AssignableNode(testCache.getClusterConfig(),
        testCache.getInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    assignableNode.assignInitBatch(assignmentSet);
    Assert.assertEquals(assignableNode.getAssignedPartitionsMap(), expectedAssignment);
    Assert.assertEquals(assignableNode.getAssignedReplicaCount(), 4);
    Assert.assertEquals(assignableNode.getProjectedHighestUtilization(Collections.EMPTY_MAP),
        16.0 / 20.0, 0.005);
    Assert.assertEquals(assignableNode.getMaxCapacity(), _capacityDataMap);
    Assert.assertEquals(assignableNode.getMaxPartition(), 5);
    Assert.assertEquals(assignableNode.getInstanceTags(), _testInstanceTags);
    Assert.assertEquals(assignableNode.getFaultZone(), _testFaultZoneId);
    Assert.assertEquals(assignableNode.getDisabledPartitionsMap(), _disabledPartitionsMap);
    Assert.assertEquals(assignableNode.getRemainingCapacity(), expectedCapacityMap);
    Assert.assertEquals(assignableNode.getAssignedReplicas(), assignmentSet);
    Assert.assertEquals(assignableNode.getAssignedPartitionsByResource(_resourceNames.get(0)),
        expectedAssignmentSet1);
    Assert.assertEquals(assignableNode.getAssignedPartitionsByResource(_resourceNames.get(1)),
        expectedAssignmentSet2);
    Assert
        .assertEquals(assignableNode.getAssignedTopStatePartitionsByResource(_resourceNames.get(0)),
            expectedTopStateAssignmentSet1);
    Assert
        .assertEquals(assignableNode.getAssignedTopStatePartitionsByResource(_resourceNames.get(1)),
            expectedTopStateAssignmentSet2);
    Assert.assertEquals(assignableNode.getAssignedTopStatePartitionsCount(),
        expectedTopStateAssignmentSet1.size() + expectedTopStateAssignmentSet2.size());

    // Test 2 - release assignment from the AssignableNode
    AssignableReplica removingReplica = new AssignableReplica(testCache.getClusterConfig(),
        testCache.getResourceConfig(_resourceNames.get(1)), _partitionNames.get(2), "MASTER", 1);
    expectedAssignment.get(_resourceNames.get(1)).remove(_partitionNames.get(2));
    expectedCapacityMap.put("item1", 9);
    expectedCapacityMap.put("item2", 18);
    Iterator<AssignableReplica> iter = assignmentSet.iterator();
    while (iter.hasNext()) {
      AssignableReplica replica = iter.next();
      if (replica.equals(removingReplica)) {
        iter.remove();
      }
    }
    expectedTopStateAssignmentSet2.remove(_partitionNames.get(2));

    assignableNode.release(removingReplica);

    Assert.assertEquals(assignableNode.getAssignedPartitionsMap(), expectedAssignment);
    Assert.assertEquals(assignableNode.getAssignedReplicaCount(), 3);
    Assert.assertEquals(assignableNode.getProjectedHighestUtilization(Collections.EMPTY_MAP),
        11.0 / 20.0, 0.005);
    Assert.assertEquals(assignableNode.getMaxCapacity(), _capacityDataMap);
    Assert.assertEquals(assignableNode.getMaxPartition(), 5);
    Assert.assertEquals(assignableNode.getInstanceTags(), _testInstanceTags);
    Assert.assertEquals(assignableNode.getFaultZone(), _testFaultZoneId);
    Assert.assertEquals(assignableNode.getDisabledPartitionsMap(), _disabledPartitionsMap);
    Assert.assertEquals(assignableNode.getRemainingCapacity(), expectedCapacityMap);
    Assert.assertEquals(assignableNode.getAssignedReplicas(), assignmentSet);
    Assert.assertEquals(assignableNode.getAssignedPartitionsByResource(_resourceNames.get(0)),
        expectedAssignmentSet1);
    Assert.assertEquals(assignableNode.getAssignedPartitionsByResource(_resourceNames.get(1)),
        expectedAssignmentSet2);
    Assert
        .assertEquals(assignableNode.getAssignedTopStatePartitionsByResource(_resourceNames.get(0)),
            expectedTopStateAssignmentSet1);
    Assert
        .assertEquals(assignableNode.getAssignedTopStatePartitionsByResource(_resourceNames.get(1)),
            expectedTopStateAssignmentSet2);
    Assert.assertEquals(assignableNode.getAssignedTopStatePartitionsCount(),
        expectedTopStateAssignmentSet1.size() + expectedTopStateAssignmentSet2.size());

    // Test 3 - add assignment to the AssignableNode
    AssignableReplica addingReplica = new AssignableReplica(testCache.getClusterConfig(),
        testCache.getResourceConfig(_resourceNames.get(1)), _partitionNames.get(2), "SLAVE", 2);
    expectedAssignment.get(_resourceNames.get(1)).add(_partitionNames.get(2));
    expectedCapacityMap.put("item1", 4);
    expectedCapacityMap.put("item2", 8);
    assignmentSet.add(addingReplica);

    assignableNode.assign(addingReplica);

    Assert.assertEquals(assignableNode.getAssignedPartitionsMap(), expectedAssignment);
    Assert.assertEquals(assignableNode.getAssignedReplicaCount(), 4);
    Assert.assertEquals(assignableNode.getProjectedHighestUtilization(Collections.EMPTY_MAP),
        16.0 / 20.0, 0.005);
    Assert.assertEquals(assignableNode.getMaxCapacity(), _capacityDataMap);
    Assert.assertEquals(assignableNode.getMaxPartition(), 5);
    Assert.assertEquals(assignableNode.getInstanceTags(), _testInstanceTags);
    Assert.assertEquals(assignableNode.getFaultZone(), _testFaultZoneId);
    Assert.assertEquals(assignableNode.getDisabledPartitionsMap(), _disabledPartitionsMap);
    Assert.assertEquals(assignableNode.getRemainingCapacity(), expectedCapacityMap);
    Assert.assertEquals(assignableNode.getAssignedReplicas(), assignmentSet);
    Assert.assertEquals(assignableNode.getAssignedPartitionsByResource(_resourceNames.get(0)),
        expectedAssignmentSet1);
    Assert.assertEquals(assignableNode.getAssignedPartitionsByResource(_resourceNames.get(1)),
        expectedAssignmentSet2);
    Assert
        .assertEquals(assignableNode.getAssignedTopStatePartitionsByResource(_resourceNames.get(0)),
            expectedTopStateAssignmentSet1);
    Assert
        .assertEquals(assignableNode.getAssignedTopStatePartitionsByResource(_resourceNames.get(1)),
            expectedTopStateAssignmentSet2);
    Assert.assertEquals(assignableNode.getAssignedTopStatePartitionsCount(),
        expectedTopStateAssignmentSet1.size() + expectedTopStateAssignmentSet2.size());
  }

  @Test
  public void testReleaseNoPartition() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();

    AssignableNode assignableNode = new AssignableNode(testCache.getClusterConfig(),
        testCache.getInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    AssignableReplica removingReplica = new AssignableReplica(testCache.getClusterConfig(),
        testCache.getResourceConfig(_resourceNames.get(1)), _partitionNames.get(2) + "non-exist",
        "MASTER", 1);

    // Release shall pass.
    assignableNode.release(removingReplica);
  }

  @Test(expectedExceptions = HelixException.class, expectedExceptionsMessageRegExp = "Resource Resource1 already has a replica with state SLAVE from partition Partition1 on node testInstanceId")
  public void testAssignDuplicateReplica() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    Set<AssignableReplica> assignmentSet = generateReplicas(testCache);

    AssignableNode assignableNode = new AssignableNode(testCache.getClusterConfig(),
        testCache.getInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    assignableNode.assignInitBatch(assignmentSet);
    AssignableReplica duplicateReplica = new AssignableReplica(testCache.getClusterConfig(),
        testCache.getResourceConfig(_resourceNames.get(0)), _partitionNames.get(0), "SLAVE", 2);
    assignableNode.assign(duplicateReplica);
  }

  @Test
  public void testParseFaultZoneNotFound() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();

    ClusterConfig testClusterConfig = new ClusterConfig("testClusterConfigId");
    testClusterConfig.setFaultZoneType("zone");
    testClusterConfig.setTopologyAwareEnabled(true);
    testClusterConfig.setTopology("/zone/");
    when(testCache.getClusterConfig()).thenReturn(testClusterConfig);

    InstanceConfig testInstanceConfig = new InstanceConfig("testInstanceConfigId");
    testInstanceConfig.setDomain("instance=testInstance");
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    instanceConfigMap.put(_testInstanceId, testInstanceConfig);
    when(testCache.getInstanceConfigMap()).thenReturn(instanceConfigMap);

    AssignableNode node = new AssignableNode(testCache.getClusterConfig(),
        testCache.getInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    Assert.assertEquals(node.getFaultZone(), "Default_zone");
  }

  @Test
  public void testParseFaultZone() throws IOException {
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

    AssignableNode assignableNode = new AssignableNode(testCache.getClusterConfig(),
        testCache.getInstanceConfigMap().get(_testInstanceId), _testInstanceId);

    Assert.assertEquals(assignableNode.getFaultZone(), "2");

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

    assignableNode = new AssignableNode(testCache.getClusterConfig(),
        testCache.getInstanceConfigMap().get(_testInstanceId), _testInstanceId);

    Assert.assertEquals(assignableNode.getFaultZone(), "2/testInstance");
  }

  @Test
  public void testDefaultInstanceCapacity() {
    ClusterConfig testClusterConfig = new ClusterConfig("testClusterConfigId");
    testClusterConfig.setDefaultInstanceCapacityMap(_capacityDataMap);

    InstanceConfig testInstanceConfig = new InstanceConfig("testInstanceConfigId");

    AssignableNode assignableNode =
        new AssignableNode(testClusterConfig, testInstanceConfig, _testInstanceId);
    Assert.assertEquals(assignableNode.getMaxCapacity(), _capacityDataMap);
  }

  @Test(expectedExceptions = HelixException.class, expectedExceptionsMessageRegExp = "The required capacity keys: \\[item2, item1, item3, AdditionalCapacityKey\\] are not fully configured in the instance: testInstanceId, capacity map: \\{item2=40, item1=20, item3=30\\}.")
  public void testIncompleteInstanceCapacity() {
    ClusterConfig testClusterConfig = new ClusterConfig("testClusterConfigId");
    List<String> requiredCapacityKeys = new ArrayList<>(_capacityDataMap.keySet());
    requiredCapacityKeys.add("AdditionalCapacityKey");
    testClusterConfig.setInstanceCapacityKeys(requiredCapacityKeys);

    InstanceConfig testInstanceConfig = new InstanceConfig("testInstanceConfigId");
    testInstanceConfig.setInstanceCapacityMap(_capacityDataMap);

    new AssignableNode(testClusterConfig, testInstanceConfig, _testInstanceId);
  }
}
