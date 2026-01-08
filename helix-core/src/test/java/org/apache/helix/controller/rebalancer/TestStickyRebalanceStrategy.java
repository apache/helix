package org.apache.helix.controller.rebalancer;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.controller.common.CapacityNode;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.strategy.StickyRebalanceStrategy;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;

public class TestStickyRebalanceStrategy {
  private static final String TEST_CLUSTER_NAME = "TestCluster";
  private static final String TEST_RESOURCE_PREFIX = "TestResource_";

  @Test
  public void testAssignmentWithGlobalPartitionLimit() {

    ResourceControllerDataProvider clusterDataCache =
        Mockito.mock(ResourceControllerDataProvider.class);
    LinkedHashMap<String, Integer> states = new LinkedHashMap<String, Integer>(2);
    states.put("OFFLINE", 0);
    states.put("ONLINE", 1);

    Set<CapacityNode> capacityNodeSet = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      CapacityNode capacityNode = new CapacityNode("Node-" + i, 1);
      capacityNodeSet.add(capacityNode);
    }

    List<String> liveNodes =
        capacityNodeSet.stream().map(CapacityNode::getInstanceName).collect(Collectors.toList());

    List<String> partitions = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      partitions.add(TEST_RESOURCE_PREFIX + "0_" + i);
    }
    when(clusterDataCache.getSimpleCapacitySet()).thenReturn(capacityNodeSet);

    StickyRebalanceStrategy greedyRebalanceStrategy = new StickyRebalanceStrategy();
    greedyRebalanceStrategy.init(TEST_RESOURCE_PREFIX + 0, partitions, states, 1);
    greedyRebalanceStrategy.computePartitionAssignment(null, liveNodes, null, clusterDataCache);

    partitions = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      partitions.add(TEST_RESOURCE_PREFIX + "1_" + i);
    }
    greedyRebalanceStrategy = new StickyRebalanceStrategy();
    greedyRebalanceStrategy.init(TEST_RESOURCE_PREFIX + 1, partitions, states, 1);
    greedyRebalanceStrategy.computePartitionAssignment(null, liveNodes, null, clusterDataCache);

    Assert.assertEquals(
        capacityNodeSet.stream().filter(node -> node.getCurrentlyAssigned() != 1).count(), 0);
  }

  @Test
  public void testStickyAssignment() {
    final int nReplicas = 4;
    final int nPartitions = 4;
    final int nNode = 16;

    ResourceControllerDataProvider clusterDataCache =
        Mockito.mock(ResourceControllerDataProvider.class);
    LinkedHashMap<String, Integer> states = new LinkedHashMap<String, Integer>(2);
    states.put("OFFLINE", 0);
    states.put("ONLINE", nReplicas);

    Set<CapacityNode> capacityNodeSet = new HashSet<>();
    for (int i = 0; i < nNode; i++) {
      CapacityNode capacityNode = new CapacityNode("Node-" + i, 1);
      capacityNodeSet.add(capacityNode);
    }

    List<String> liveNodes =
        capacityNodeSet.stream().map(CapacityNode::getInstanceName).collect(Collectors.toList());

    List<String> partitions = new ArrayList<>();
    for (int i = 0; i < nPartitions; i++) {
      partitions.add(TEST_RESOURCE_PREFIX + i);
    }
    when(clusterDataCache.getSimpleCapacitySet()).thenReturn(capacityNodeSet);

    // Populate previous assignment with currentMapping
    Map<String, Map<String, String>> currentMapping = new HashMap<>();
    currentMapping.put(TEST_RESOURCE_PREFIX + "0", new HashMap<>());
    currentMapping.get(TEST_RESOURCE_PREFIX + "0").put("Node-0", "ONLINE");
    currentMapping.get(TEST_RESOURCE_PREFIX + "0").put("Node-2", "ONLINE");
    currentMapping.get(TEST_RESOURCE_PREFIX + "0").put("Node-4", "ONLINE");
    currentMapping.get(TEST_RESOURCE_PREFIX + "0").put("Node-6", "ONLINE");
    currentMapping.put(TEST_RESOURCE_PREFIX + "2", new HashMap<>());
    currentMapping.get(TEST_RESOURCE_PREFIX + "2").put("Node-1", "ONLINE");
    currentMapping.get(TEST_RESOURCE_PREFIX + "2").put("Node-5", "ONLINE");
    currentMapping.get(TEST_RESOURCE_PREFIX + "2").put("Node-8", "ONLINE");

    StickyRebalanceStrategy greedyRebalanceStrategy = new StickyRebalanceStrategy();
    greedyRebalanceStrategy.init(TEST_RESOURCE_PREFIX + 0, partitions, states, 1);
    ZNRecord shardAssignment =
        greedyRebalanceStrategy.computePartitionAssignment(null, liveNodes, currentMapping,
            clusterDataCache);

    // Assert the existing assignment won't be changed
    Assert.assertEquals(currentMapping.get(TEST_RESOURCE_PREFIX + "0").keySet(),
        new HashSet<>(shardAssignment.getListField(TEST_RESOURCE_PREFIX + "0")));
    Assert.assertTrue(shardAssignment.getListField(TEST_RESOURCE_PREFIX + "2")
        .containsAll(currentMapping.get(TEST_RESOURCE_PREFIX + "2").keySet()));
  }

  @Test
  public void testStickyAssignmentMultipleTimes() {
    final int nReplicas = 4;
    final int nPartitions = 4;
    final int nNode = 12;

    ResourceControllerDataProvider clusterDataCache =
        Mockito.mock(ResourceControllerDataProvider.class);
    LinkedHashMap<String, Integer> states = new LinkedHashMap<String, Integer>(2);
    states.put("OFFLINE", 0);
    states.put("ONLINE", nReplicas);

    Set<CapacityNode> capacityNodeSet = new HashSet<>();
    for (int i = 0; i < nNode; i++) {
      CapacityNode capacityNode = new CapacityNode("Node-" + i, 1);
      capacityNodeSet.add(capacityNode);
    }

    List<String> liveNodes =
        capacityNodeSet.stream().map(CapacityNode::getInstanceName).collect(Collectors.toList());

    List<String> partitions = new ArrayList<>();
    for (int i = 0; i < nPartitions; i++) {
      partitions.add(TEST_RESOURCE_PREFIX + i);
    }
    when(clusterDataCache.getSimpleCapacitySet()).thenReturn(capacityNodeSet);

    StickyRebalanceStrategy stickyRebalanceStrategy = new StickyRebalanceStrategy();
    stickyRebalanceStrategy.init(TEST_RESOURCE_PREFIX + 0, partitions, states, 1);
    // First round assignment computation:
    // 1. Without previous assignment (currentMapping is null)
    // 2. Without enough assignable nodes
    ZNRecord firstRoundShardAssignment =
        stickyRebalanceStrategy.computePartitionAssignment(null, liveNodes, null, clusterDataCache);

    // Assert only 3 partitions are fulfilled with assignment
    Assert.assertEquals(firstRoundShardAssignment.getListFields().entrySet().stream()
        .filter(e -> e.getValue().size() == nReplicas).count(), 3);

    // Assign 4 more nodes which is used in second round assignment computation
    for (int i = nNode; i < nNode + 4; i++) {
      CapacityNode capacityNode = new CapacityNode("Node-" + i, 1);
      capacityNodeSet.add(capacityNode);
    }

    liveNodes =
        capacityNodeSet.stream().map(CapacityNode::getInstanceName).collect(Collectors.toList());

    // Populate previous assignment (currentMapping) with first round assignment computation result
    Map<String, Map<String, String>> currentMapping = new HashMap<>();
    firstRoundShardAssignment.getListFields().entrySet().stream()
        .filter(e -> e.getValue().size() == nReplicas).forEach(e -> {
          currentMapping.put(e.getKey(), new HashMap<>());
          for (String nodeId : e.getValue()) {
            currentMapping.get(e.getKey()).put(nodeId, "ONLINE");
          }
        });

    // Second round assignment computation:
    // 1. With previous assignment (currentMapping)
    // 2. With enough assignable nodes
    ZNRecord secondRoundShardAssignment =
        stickyRebalanceStrategy.computePartitionAssignment(null, liveNodes, currentMapping,
            clusterDataCache);

    // Assert all partitions have been assigned with enough replica
    Assert.assertEquals(secondRoundShardAssignment.getListFields().entrySet().stream()
        .filter(e -> e.getValue().size() == nReplicas).count(), nPartitions);
    // For previously existing assignment, assert there is no assignment change
    currentMapping.forEach((partition, nodeMapping) -> {
      Assert.assertEquals(nodeMapping.keySet(),
          new HashSet<>(secondRoundShardAssignment.getListField(partition)));
    });
  }
}
