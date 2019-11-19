package org.apache.helix.controller.strategy.crushMapping;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.helix.controller.rebalancer.strategy.crushMapping.CardDealingAdjustmentAlgorithmV2;
import org.apache.helix.controller.rebalancer.topology.InstanceNode;
import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.controller.rebalancer.topology.Topology;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCardDealingAdjustmentAlgorithmV2 {
  private static int DEFAULT_REPLICA_COUNT = 3;
  private static int DEFAULT_RANDOM_SEED = 10;
  private static int NUMS_ZONES = 3;
  private static int NUM_INSTANCES_PER_ZONE = 3;
  private static int NUM_TOTAL_INSTANCES;
  private static int[][] DEFAULT_ZONES = new int[NUMS_ZONES][NUM_INSTANCES_PER_ZONE];

  static {
    for (int i = 0; i < NUMS_ZONES; i++) {
      for (int j = 0; j < NUM_INSTANCES_PER_ZONE; j++) {
        DEFAULT_ZONES[i][j] = i * NUMS_ZONES + j + 1;
      }
    }

    NUM_TOTAL_INSTANCES = NUMS_ZONES * NUM_INSTANCES_PER_ZONE;
  }

  private Topology _topology;

  @BeforeClass
  public void setUpTopology() {
    _topology = mock(Topology.class);
    System.out.println("Default ZONES: " + Arrays.deepToString(DEFAULT_ZONES));
    when(_topology.getFaultZones()).thenReturn(createFaultZones(DEFAULT_ZONES));
  }

  private List<Node> createFaultZones(int[][] instancesMap) {
    List<Node> faultZones = new ArrayList<>();
    int zoneId = 0;
    for (int[] instances : instancesMap) {
      Node zoneNode = new Node();
      zoneNode.setName("zone" + zoneId);
      zoneNode.setId(zoneId++);
      int zoneWeight = 0;
      for (int instanceId : instances) {
        Node node = new Node();
        node.setName("instance" + instanceId);
        node.setId(instanceId);
        Node instanceNode = new InstanceNode(node, "instance" + instanceId);
        // use instance id (integer) as the weight for ease of testing
        instanceNode.addWeight(instanceId);
        zoneNode.addChild(instanceNode);
        zoneWeight += instanceNode.getWeight();
      }
      zoneNode.addWeight(zoneWeight);

      faultZones.add(zoneNode);
    }

    return faultZones;
  }

  @Test(description = "Verify a few properties after algorithm object is created")
  public void testAlgorithmConstructor() {
    CardDealingAdjustmentAlgorithmV2Accessor algorithm =
        new CardDealingAdjustmentAlgorithmV2Accessor(_topology, DEFAULT_REPLICA_COUNT,
            CardDealingAdjustmentAlgorithmV2.Mode.EVENNESS);
    Map<Node, Long> instanceWeights = algorithm.getInstanceWeight();
    // verify weight is set correctly
    for (Map.Entry<Node, Long> entry : instanceWeights.entrySet()) {
      if (entry.getKey().getId() != entry.getValue()) {
        Assert.fail(String.format("%s %s should have weight of %s", entry.getKey().getName(),
            entry.getKey().getId(), entry.getValue()));
      }
    }
    Map<Node, Long> faultZoneWeights = algorithm.getFaultZoneWeight();
    Map<Node, Set<String>> faultZonePartitionMap = algorithm.getFaultZonePartitionMap();
    Set<Node> faultZones = faultZoneWeights.keySet();

    Assert.assertEquals(faultZoneWeights.size(), NUMS_ZONES);
    Assert.assertEquals(faultZonePartitionMap.keySet(), faultZones);

    long sum = 0;
    for (long weight : faultZoneWeights.values()) {
      sum += weight;
    }
    // verify total weight is computed correct
    if (sum != algorithm.getTotalWeight()) {
      Assert.fail(String.format("total weight %s != total weight of zones %s",
          algorithm.getTotalWeight(), sum));
    }
    Map<Node, Node> instanceFaultZone = algorithm.getInstanceFaultZone();
    Assert.assertEquals(instanceFaultZone.size(), NUM_TOTAL_INSTANCES);

    // verify zone mapping is correct
    for (Node zone : faultZones) {
      long zoneId = zone.getId();
      List<Node> instanceNodes = zone.getChildren();
      Set<Long> actualInstanceIds = new HashSet<>();
      for (Node node : instanceNodes) {
        actualInstanceIds.add(node.getId());
      }
      Set<Long> expectedInstanceIds = new HashSet<>();
      for (int i = 0; i < NUM_INSTANCES_PER_ZONE; i++) {
        expectedInstanceIds.add(NUMS_ZONES * zoneId + i + 1);
      }
      Assert.assertEquals(instanceNodes.size(), NUM_INSTANCES_PER_ZONE);
      Assert.assertEquals(actualInstanceIds, expectedInstanceIds);
    }
  }

  @DataProvider
  public static Object[][] stableComputingVerification() {
    return new Object[][] {
        // replica, repeatTimes, seed, true: evenness false: less movement
        {
            1, 10, 0, true
        }, {
            2, 10, 10, true
        }, {
            3, 10, 100, true
        }, {
            1, 10, 0, true
        }, {
            2, 10, 10, true
        }, {
            3, 10, 100, true
        }, {
            1, 10, 0, true
        }, {
            2, 10, 10, true
        }, {
            3, 10, 100, true
        }, {
            1, 10, 0, false
        }, {
            2, 10, 10, false
        }, {
            3, 10, 100, false
        }, {
            1, 10, 0, false
        }, {
            2, 10, 10, false
        }, {
            3, 10, 100, false
        }, {
            1, 10, 0, false
        }, {
            2, 10, 10, false
        }, {
            3, 10, 100, false
        }
    };
  }

  @Test(description = "Compute mapping multiple times, the mapping of each time should be same", dataProvider = "stableComputingVerification")
  public void testStableComputeMappingForMultipleTimes(int replica, int repeatTimes, int seed,
      boolean isEvennessPreferred) {
    CardDealingAdjustmentAlgorithmV2.Mode preference =
        isEvennessPreferred ? CardDealingAdjustmentAlgorithmV2.Mode.EVENNESS
            : CardDealingAdjustmentAlgorithmV2.Mode.MINIMIZE_MOVEMENT;
    CardDealingAdjustmentAlgorithmV2Accessor algorithm =
        new CardDealingAdjustmentAlgorithmV2Accessor(_topology, replica, preference);
    Set<Node> instanceNodes = algorithm.getInstanceFaultZone().keySet();
    Map<Node, List<String>> nodeToPartitions = new HashMap<>();
    for (Node instanceNode : instanceNodes) {
      int start = (int) instanceNode.getId();
      int end = start + (int) instanceNode.getWeight();
      nodeToPartitions.put(instanceNode, createDummyPartitions(start, end));
    }

    Map<Long, Set<String>> oldSimpleMapping = getSimpleMapping(nodeToPartitions);
    Map<Long, Set<String>> lastCalculatedDifference = new HashMap<>();
    while (repeatTimes > 0) {
      System.out.println(String.format("Round %s replica %s algorithm seed: %s preference: %s ",
          repeatTimes, replica, seed, preference));
      // deep clone the original mapping
      Map<Node, List<String>> newMapping = new HashMap<>();
      for (Map.Entry<Node, List<String>> entry : nodeToPartitions.entrySet()) {
        newMapping.put(entry.getKey(), new ArrayList<>(entry.getValue()));
      }
      new CardDealingAdjustmentAlgorithmV2(_topology, replica, preference)
          .computeMapping(newMapping, seed);

      Map<Long, Set<String>> newDifference =
          getDifference(oldSimpleMapping, getSimpleMapping(newMapping));

      if (!lastCalculatedDifference.isEmpty() && !newDifference.equals(lastCalculatedDifference)) {
        Assert.fail("Different mapping of the same input");
      }

      lastCalculatedDifference = newDifference;
      repeatTimes -= 1;
    }
  }

  @DataProvider
  public static Object[][] replicas() {
    return new Object[][] {
        {
            1
        }, {
            2
        }, {
            3
        }, {
            4
        }, {
            5
        }
    };
  }

  @Test(description = "Test performance given different replica count", dataProvider = "replicas")
  public void testComputeMappingForDifferentReplicas(int replica) {
    CardDealingAdjustmentAlgorithmV2Accessor algorithm =
        new CardDealingAdjustmentAlgorithmV2Accessor(_topology, replica,
            CardDealingAdjustmentAlgorithmV2.Mode.EVENNESS);
    Set<Node> instanceNodes = algorithm.getInstanceFaultZone().keySet();
    Map<Node, List<String>> nodeToPartitions = new HashMap<>();
    for (Node instanceNode : instanceNodes) {
      int start = (int) instanceNode.getId();
      int end = start + (int) instanceNode.getWeight();
      nodeToPartitions.put(instanceNode, createDummyPartitions(start, end));
    }
    Map<Long, Set<String>> oldSimpleMapping = getSimpleMapping(nodeToPartitions);
    boolean isAllAssigned = algorithm.computeMapping(nodeToPartitions, DEFAULT_RANDOM_SEED);
    Map<Long, Set<String>> difference =
        getDifference(oldSimpleMapping, getSimpleMapping(nodeToPartitions));
    int totalMovements = 0;
    for (Set<String> value : difference.values()) {
      totalMovements += value.size();
    }

    // These are the previously calculated results, keep them to make sure consistency
    Map<Integer, Integer> expected = ImmutableMap.of(1, 8, 2, 8, 3, 21, 4, 0, 5, 0);

    if (totalMovements != expected.get(replica)) {
      Assert.fail(String.format("Total movements: %s != expected %s, replica: %s", totalMovements,
          expected.get(replica), replica));
    }
  }

  @Test(description = "Test performance given different preference (evenness or less movements)")
  public void testComputeMappingForDifferentPreference() {
    CardDealingAdjustmentAlgorithmV2Accessor algorithm1 =
        new CardDealingAdjustmentAlgorithmV2Accessor(_topology, DEFAULT_REPLICA_COUNT,
            CardDealingAdjustmentAlgorithmV2.Mode.EVENNESS);
    CardDealingAdjustmentAlgorithmV2Accessor algorithm2 =
        new CardDealingAdjustmentAlgorithmV2Accessor(_topology, DEFAULT_REPLICA_COUNT,
            CardDealingAdjustmentAlgorithmV2.Mode.MINIMIZE_MOVEMENT);
    Set<Node> instanceNodes = algorithm1.getInstanceFaultZone().keySet();
    Map<Node, List<String>> nodeToPartitions = new HashMap<>();
    for (Node instanceNode : instanceNodes) {
      int start = (int) instanceNode.getId();
      int end = start + (int) instanceNode.getWeight();
      nodeToPartitions.put(instanceNode, createDummyPartitions(start, end));
    }
    Map<Long, Set<String>> oldSimpleMapping = getSimpleMapping(nodeToPartitions);
    // deep clone the original mapping
    Map<Node, List<String>> newMapping = new HashMap<>();
    for (Map.Entry<Node, List<String>> entry : nodeToPartitions.entrySet()) {
      newMapping.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }
    boolean isAllAssigned1 = algorithm1.computeMapping(nodeToPartitions, DEFAULT_RANDOM_SEED);
    boolean isAllAssigned2 = algorithm2.computeMapping(newMapping, DEFAULT_RANDOM_SEED);

    int movement1 = getTotalMovements(oldSimpleMapping, getSimpleMapping(nodeToPartitions));
    int movement2 = getTotalMovements(oldSimpleMapping, getSimpleMapping(newMapping));

    System.out.println(String.format("Total movements: %s, isAllAssigned: %s, preference: %s",
        movement1, isAllAssigned1, CardDealingAdjustmentAlgorithmV2.Mode.EVENNESS));
    System.out.println(String.format("Total movements: %s, isAllAssigned: %s, preference: %s",
        movement2, isAllAssigned2, CardDealingAdjustmentAlgorithmV2.Mode.MINIMIZE_MOVEMENT));
    Assert.assertTrue(movement1 >= movement2);
  }

  @Test
  public void testComputeMappingWhenZeroWeightInstance() {
    when(_topology.getFaultZones()).thenReturn(createFaultZones(new int[][] {
        {
            0, 1
        }, // zone0: instance id = 0, weight = 0; instance id = 1, weight = 1; zone weight = 1
        {
            2, 3
        }, // zone1: instance id = 2, weight = 2; instance id = 3, weight = 3; zone weight = 5,
        {
            4, 5
        } // zone2: instance id = 4, weight = 4; instance id = 5, weight = 5; zone weight = 9
    }));

    CardDealingAdjustmentAlgorithmV2Accessor algorithm =
        new CardDealingAdjustmentAlgorithmV2Accessor(_topology, DEFAULT_REPLICA_COUNT,
            CardDealingAdjustmentAlgorithmV2.Mode.EVENNESS);
    Set<Node> instanceNodes = algorithm.getInstanceFaultZone().keySet();
    Map<Node, List<String>> nodeToPartitions = new HashMap<>();
    // assign 2 partitions to each instance
    // zone0 with weight = 1 will be overloaded, the other two zones should have spaces left
    for (Node instanceNode : instanceNodes) {
      nodeToPartitions.put(instanceNode, createDummyPartitions(0, 2));
    }
    Map<Long, Set<String>> oldSimpleMapping = getSimpleMapping(nodeToPartitions);
    boolean isAssigned = algorithm.computeMapping(nodeToPartitions, DEFAULT_RANDOM_SEED);
    Map<Long, Set<String>> newSimpleMapping = getSimpleMapping(nodeToPartitions);

    System.out.println("old mapping" + oldSimpleMapping);
    System.out.println("new mapping" + newSimpleMapping);
    Assert.assertTrue(newSimpleMapping.get(0L).isEmpty());
  }

  @Test
  public void testComputeMappingWhenZeroWeightZone() {
    when(_topology.getFaultZones()).thenReturn(createFaultZones(new int[][] {
        {
            0
        }, // zone0: instance id = 0, weight = 0; zone weight = 0
        {
            2, 3
        }, // zone1: instance id = 2, weight = 2; instance id = 3, weight = 3; zone weight = 5,
        {
            4, 5
        } // zone2: instance id = 4, weight = 4; instance id = 5, weight = 5; zone weight = 9
    }));

    CardDealingAdjustmentAlgorithmV2Accessor algorithm =
        new CardDealingAdjustmentAlgorithmV2Accessor(_topology, DEFAULT_REPLICA_COUNT,
            CardDealingAdjustmentAlgorithmV2.Mode.EVENNESS);
    Set<Node> instanceNodes = algorithm.getInstanceFaultZone().keySet();
    Map<Node, List<String>> nodeToPartitions = new HashMap<>();
    for (Node instanceNode : instanceNodes) {
      nodeToPartitions.put(instanceNode, createDummyPartitions(0, 2)); // assign 2 partitions to
                                                                       // each instance
    }
    Map<Long, Set<String>> oldSimpleMapping = getSimpleMapping(nodeToPartitions);
    boolean isAssigned = algorithm.computeMapping(nodeToPartitions, DEFAULT_RANDOM_SEED);
    Map<Long, Set<String>> newSimpleMapping = getSimpleMapping(nodeToPartitions);

    System.out.println("old mapping" + oldSimpleMapping);
    System.out.println("new mapping" + newSimpleMapping);
    Assert.assertTrue(newSimpleMapping.get(0L).isEmpty());
  }

  private int getTotalMovements(Map<Long, Set<String>> oldSimpleMapping,
      Map<Long, Set<String>> newSimpleMapping) {
    Map<Long, Set<String>> difference = getDifference(oldSimpleMapping, newSimpleMapping);
    int totalMovements = 0;
    for (Set<String> value : difference.values()) {
      totalMovements += value.size();
    }

    return totalMovements;
  }

  private Map<Long, Set<String>> getDifference(Map<Long, Set<String>> oldSimpleMapping,
      Map<Long, Set<String>> newSimpleMapping) {
    Map<Long, Set<String>> difference = new HashMap<>();
    for (long instanceId : newSimpleMapping.keySet()) {
      Set<String> addedPartitions = new HashSet<>();
      Set<String> lostPartitions = new HashSet<>();
      for (String partition : Sets.difference(newSimpleMapping.get(instanceId),
          oldSimpleMapping.get(instanceId))) {
        addedPartitions.add("+" + partition);
      }
      for (String partition : Sets.difference(oldSimpleMapping.get(instanceId),
          newSimpleMapping.get(instanceId))) {
        addedPartitions.add("-" + partition);
      }
      Set<String> changePartitions = Sets.union(addedPartitions, lostPartitions);
      difference.put(instanceId, changePartitions);
    }

    return difference;
  }

  private Map<Long, Set<String>> getSimpleMapping(Map<Node, List<String>> nodeToPartitions) {
    Map<Long, Set<String>> mapping = new HashMap<>();
    for (Map.Entry<Node, List<String>> entry : nodeToPartitions.entrySet()) {
      mapping.put(entry.getKey().getId(), new HashSet<>(entry.getValue()));
    }

    return mapping;
  }

  private List<String> createDummyPartitions(int start, int end) {
    List<String> dummyPartitions = new ArrayList<>();
    for (int i = start; i <= end; i++) {
      dummyPartitions.add("Partition_" + i);
    }

    return dummyPartitions;
  }

  // The accessor class to get protected fields of {@link CardDealingAdjustmentAlgorithmV2}
  private static class CardDealingAdjustmentAlgorithmV2Accessor
      extends CardDealingAdjustmentAlgorithmV2 {
    CardDealingAdjustmentAlgorithmV2Accessor(Topology topology, int replica, Mode mode) {
      super(topology, replica, mode);
    }

    Map<Node, Node> getInstanceFaultZone() {
      return _instanceFaultZone;
    }

    Map<Node, Long> getInstanceWeight() {
      return _instanceWeight;
    }

    long getTotalWeight() {
      return _totalWeight;
    }

    Map<Node, Long> getFaultZoneWeight() {
      return _faultZoneWeight;
    }

    Map<Node, Set<String>> getFaultZonePartitionMap() {
      return _faultZonePartitionMap;
    }
  }
}
