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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.controller.common.CapacityNode;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.strategy.GreedyRebalanceStrategy;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;

public class TestGreedyRebalanceStrategy {
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
      CapacityNode capacityNode = new CapacityNode("Node-" + i);
      capacityNode.setCapacity(1);
      capacityNodeSet.add(capacityNode);
    }

    List<String> liveNodes =
        capacityNodeSet.stream().map(CapacityNode::getId).collect(Collectors.toList());

    List<String> partitions = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      partitions.add(TEST_RESOURCE_PREFIX + "0_" + i);
    }
    when(clusterDataCache.getSimpleCapacitySet()).thenReturn(capacityNodeSet);

    GreedyRebalanceStrategy greedyRebalanceStrategy = new GreedyRebalanceStrategy();
    greedyRebalanceStrategy.init(TEST_RESOURCE_PREFIX + 0, partitions, states, 1);
    greedyRebalanceStrategy.computePartitionAssignment(null, liveNodes, null, clusterDataCache);

    partitions = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      partitions.add(TEST_RESOURCE_PREFIX + "1_" + i);
    }
    greedyRebalanceStrategy = new GreedyRebalanceStrategy();
    greedyRebalanceStrategy.init(TEST_RESOURCE_PREFIX + 1, partitions, states, 1);
    greedyRebalanceStrategy.computePartitionAssignment(null, liveNodes, null, clusterDataCache);

    Assert.assertEquals(
        capacityNodeSet.stream().filter(node -> node.getCurrentlyAssigned() != 1).count(), 0);
  }
}
