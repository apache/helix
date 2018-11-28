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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceHardConstraint;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceSoftConstraint;
import org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider;
import org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.constraint.PartitionWeightAwareEvennessConstraint;
import org.apache.helix.controller.rebalancer.constraint.TotalCapacityConstraint;
import org.apache.helix.controller.rebalancer.constraint.dataprovider.MockCapacityProvider;
import org.apache.helix.controller.rebalancer.constraint.dataprovider.MockPartitionWeightProvider;
import org.apache.helix.controller.rebalancer.strategy.ConstraintRebalanceStrategy;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestConstraintRebalanceStrategy {
  private static Logger _logger = LoggerFactory.getLogger(TestConstraintRebalanceStrategy.class);

  final String resourceNamePrefix = "resource";
  final int nParticipants = 40;
  final int nResources = 20;
  final int nPartitions = 100;
  final int nReplicas = 3;
  final int defaultCapacity = 6000; // total = 6000*40 = 240000
  final int resourceWeight = 10; // total = 20*100*3*10 = 60000
  final String topState = "ONLINE";

  final List<String> resourceNames = new ArrayList<>();
  final List<String> instanceNames = new ArrayList<>();
  final List<String> partitions = new ArrayList<>(nPartitions);

  final ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
  final LinkedHashMap<String, Integer> states = new LinkedHashMap<>(2);

  @BeforeClass
  public void beforeClass() {
    for (int i = 0; i < nResources; i++) {
      resourceNames.add(resourceNamePrefix + i);
    }
    for (int i = 0; i < nParticipants; i++) {
      instanceNames.add("node" + i);
    }
    for (int i = 0; i < nPartitions; i++) {
      partitions.add(Integer.toString(i));
    }

    setupMockCluster();
  }

  private void setupMockCluster() {
    List<LiveInstance> liveInstanceList = new ArrayList<>();
    Map<String, InstanceConfig> instanceConfigs = new HashMap<>();
    for (String instance : instanceNames) {
      LiveInstance liveInstance = new LiveInstance(instance);
      liveInstanceList.add(liveInstance);
      InstanceConfig config = new InstanceConfig(instance);
      instanceConfigs.put(instance, config);
    }
    cache.setLiveInstances(liveInstanceList);
    cache.setInstanceConfigMap(instanceConfigs);
    ClusterConfig clusterConfig = new ClusterConfig("test");
    clusterConfig.setTopologyAwareEnabled(false);
    cache.setClusterConfig(clusterConfig);

    states.put("OFFLINE", 0);
    states.put(topState, nReplicas);
  }

  private Map<String, Map<String, Map<String, String>>> calculateAssignment(
      List<AbstractRebalanceHardConstraint> hardConstraints,
      List<AbstractRebalanceSoftConstraint> softConstraints) {
    Map<String, Map<String, Map<String, String>>> result = new HashMap<>();

    ConstraintRebalanceStrategy strategy =
        new ConstraintRebalanceStrategy(hardConstraints, softConstraints);

    for (String resourceName : resourceNames) {
      Map<String, Map<String, String>> partitionMap = new HashMap<>();

      strategy.init(resourceName, partitions, states, Integer.MAX_VALUE);
      partitionMap.putAll(strategy.computePartitionAssignment(instanceNames, instanceNames,
          new HashMap<String, Map<String, String>>(), cache).getMapFields());
      result.put(resourceName, partitionMap);
    }
    return result;
  }

  private Map<String, Integer> checkPartitionUsage(
      Map<String, Map<String, Map<String, String>>> assignment,
      PartitionWeightProvider weightProvider) {
    Map<String, Integer> weightCount = new HashMap<>();
    for (String resource : assignment.keySet()) {
      Map<String, Map<String, String>> partitionMap = assignment.get(resource);
      for (String partition : partitionMap.keySet()) {
        // check states
        Map<String, Integer> stateCount = new HashMap<>(states);
        Map<String, String> stateMap = partitionMap.get(partition);
        for (String state : stateMap.values()) {
          Assert.assertTrue(stateCount.containsKey(state));
          stateCount.put(state, stateCount.get(state) - 1);
        }
        for (int count : stateCount.values()) {
          Assert.assertEquals(count, 0);
        }

        // report weight
        int partitionWeight = weightProvider.getPartitionWeight(resource, partition);
        for (String instance : partitionMap.get(partition).keySet()) {
          if (!weightCount.containsKey(instance)) {
            weightCount.put(instance, partitionWeight);
          } else {
            weightCount.put(instance, weightCount.get(instance) + partitionWeight);
          }
        }
      }
    }
    return weightCount;
  }

  @Test
  public void testEvenness() {
    // capacity / weight
    Map<String, Integer> capacity = new HashMap<>();
    for (String instance : instanceNames) {
      capacity.put(instance, defaultCapacity);
    }

    PartitionWeightProvider weightProvider = new MockPartitionWeightProvider(resourceWeight);
    CapacityProvider capacityProvider = new MockCapacityProvider(capacity, 0);

    TotalCapacityConstraint capacityConstraint =
        new TotalCapacityConstraint(weightProvider, capacityProvider);
    PartitionWeightAwareEvennessConstraint evenConstraint =
        new PartitionWeightAwareEvennessConstraint(weightProvider, capacityProvider);

    Map<String, Map<String, Map<String, String>>> assignment = calculateAssignment(
        Collections.<AbstractRebalanceHardConstraint>singletonList(capacityConstraint),
        Collections.<AbstractRebalanceSoftConstraint>singletonList(evenConstraint));
    Map<String, Integer> weightCount = checkPartitionUsage(assignment, weightProvider);

    int max = Collections.max(weightCount.values());
    int min = Collections.min(weightCount.values());
    // Since the accuracy of Default evenness constraint is 0.01, diff should be 1/100 of participant capacity in max.
    Assert.assertTrue((max - min) <= defaultCapacity / 100);
  }

  @Test
  public void testEvennessByDefaultConstraint() {
    Map<String, Map<String, Map<String, String>>> result = new HashMap<>();

    ConstraintRebalanceStrategy strategy = new ConstraintRebalanceStrategy();

    for (String resourceName : resourceNames) {
      Map<String, Map<String, String>> partitionMap = new HashMap<>();

      strategy.init(resourceName, partitions, states, Integer.MAX_VALUE);
      partitionMap.putAll(strategy.computePartitionAssignment(instanceNames, instanceNames,
          new HashMap<String, Map<String, String>>(), cache).getMapFields());
      result.put(resourceName, partitionMap);
    }

    Map<String, Integer> weightCount = checkPartitionUsage(result, new PartitionWeightProvider() {
      @Override
      public int getPartitionWeight(String resource, String partition) {
        return 1;
      }
    });
    int max = Collections.max(weightCount.values());
    int min = Collections.min(weightCount.values());
    // Since the accuracy of Default evenness constraint is 0.01, diff should be 1/100 of participant capacity in max.
    Assert.assertTrue((max - min) <= defaultCapacity / 100);
  }

  @Test
  public void testCapacityAwareEvenness() {
    // capacity / weight
    int totalBucket = 0;
    Map<String, Integer> capacity = new HashMap<>();
    for (int i = 0; i < instanceNames.size(); i++) {
      capacity.put(instanceNames.get(i), defaultCapacity * (1 + i % 3));
      totalBucket += 1 + i % 3;
    }
    int partitionWeightGranularity = (int) (resourceWeight * 1.5);
    int totalPartitionWeight = 0;
    Random ran = new Random(System.currentTimeMillis());
    Map<String, Map<String, Integer>> partitionWeightMap = new HashMap<>();
    for (String resource : resourceNames) {
      Map<String, Integer> weights = new HashMap<>();
      for (String partition : partitions) {
        int weight = resourceWeight / 2 + ran.nextInt(resourceWeight);
        weights.put(partition, weight);
        totalPartitionWeight += weight * nReplicas;
      }
      partitionWeightMap.put(resource, weights);
    }

    PartitionWeightProvider weightProvider =
        new MockPartitionWeightProvider(partitionWeightMap, resourceWeight);
    CapacityProvider capacityProvider = new MockCapacityProvider(capacity, 0);

    PartitionWeightAwareEvennessConstraint evenConstraint =
        new PartitionWeightAwareEvennessConstraint(weightProvider, capacityProvider);

    Map<String, Map<String, Map<String, String>>> assignment =
        calculateAssignment(Collections.EMPTY_LIST,
            Collections.<AbstractRebalanceSoftConstraint>singletonList(evenConstraint));
    Map<String, Integer> weightCount = checkPartitionUsage(assignment, weightProvider);

    for (int i = 0; i < instanceNames.size(); i++) {
      String instanceName = instanceNames.get(i);
      int expectedUsage = (int) ((double) totalPartitionWeight) / totalBucket * (1 + i % 3);
      int realUsage = weightCount.get(instanceName);
      // When have different capacity, calculation in the rebalance algorithm would have more fractions, so lose the restriction to 90% to 110% compared with the ideal value.
      Assert.assertTrue((expectedUsage - partitionWeightGranularity) * 0.9 <= realUsage
          && (expectedUsage + partitionWeightGranularity) * 1.1 >= realUsage);
    }
  }

  @Test
  public void testHardConstraintFails() {
    // capacity / weight
    Map<String, Integer> capacity = new HashMap<>();
    for (String instance : instanceNames) {
      // insufficient capacity
      capacity.put(instance, defaultCapacity / 100);
    }

    PartitionWeightProvider weightProvider = new MockPartitionWeightProvider(resourceWeight);
    CapacityProvider capacityProvider = new MockCapacityProvider(capacity, 0);

    TotalCapacityConstraint capacityConstraint =
        new TotalCapacityConstraint(weightProvider, capacityProvider);

    try {
      calculateAssignment(
          Collections.<AbstractRebalanceHardConstraint>singletonList(capacityConstraint),
          Collections.EMPTY_LIST);
      Assert.fail("Assignment should fail because of insufficient capacity.");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test(dependsOnMethods = "testHardConstraintFails")
  public void testConflictConstraint() {
    // capacity / weight
    Map<String, Integer> capacity = new HashMap<>();
    for (String instance : instanceNames) {
      // insufficient capacity
      capacity.put(instance, defaultCapacity);
    }

    PartitionWeightProvider weightProvider = new MockPartitionWeightProvider(resourceWeight);
    CapacityProvider capacityProvider = new MockCapacityProvider(capacity, 0);

    TotalCapacityConstraint normalCapacityConstraint =
        new TotalCapacityConstraint(weightProvider, capacityProvider);
    TotalCapacityConstraint conflictingCapacityConstraint =
        new TotalCapacityConstraint(weightProvider,
            new MockCapacityProvider(Collections.EMPTY_MAP, 0));
    List<AbstractRebalanceHardConstraint> constraints = new ArrayList<>();
    constraints.add(normalCapacityConstraint);
    constraints.add(conflictingCapacityConstraint);

    try {
      calculateAssignment(constraints, Collections.EMPTY_LIST);
      Assert.fail("Assignment should fail because of the conflicting capacity constraint.");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test(dependsOnMethods = "testEvenness")
  public void testSoftConstraintFails() {
    // capacity / weight
    Map<String, Integer> capacity = new HashMap<>();
    for (String instance : instanceNames) {
      // insufficient capacity
      capacity.put(instance, defaultCapacity / 50);
    }

    PartitionWeightProvider weightProvider = new MockPartitionWeightProvider(resourceWeight);
    CapacityProvider capacityProvider = new MockCapacityProvider(capacity, 0);

    PartitionWeightAwareEvennessConstraint evenConstraint =
        new PartitionWeightAwareEvennessConstraint(weightProvider, capacityProvider);

    Map<String, Map<String, Map<String, String>>> assignment =
        calculateAssignment(Collections.EMPTY_LIST,
            Collections.<AbstractRebalanceSoftConstraint>singletonList(evenConstraint));
    Map<String, Integer> weightCount = checkPartitionUsage(assignment, weightProvider);

    int max = Collections.max(weightCount.values());
    int min = Collections.min(weightCount.values());
    // Since the accuracy of Default evenness constraint is 0.01, diff should be 1/100 of participant capacity in max.
    Assert.assertTrue((max - min) <= defaultCapacity / 100);
  }

  @Test(dependsOnMethods = "testEvenness")
  public void testRebalanceWithPreferredAssignment() {
    // capacity / weight
    Map<String, Integer> capacity = new HashMap<>();
    for (String instance : instanceNames) {
      capacity.put(instance, defaultCapacity);
    }

    PartitionWeightProvider weightProvider = new MockPartitionWeightProvider(resourceWeight);
    CapacityProvider capacityProvider = new MockCapacityProvider(capacity, 0);

    PartitionWeightAwareEvennessConstraint evenConstraint =
        new PartitionWeightAwareEvennessConstraint(weightProvider, capacityProvider);

    // inject valid partition assignment for one resources into preferred assignment.
    List<String> instances = instanceNames.subList(0, nReplicas);
    Map<String, Map<String, String>> preferredPartitionAssignment = new HashMap<>();
    Map<String, String> replicaState = new HashMap<>();
    for (String instance : instances) {
      replicaState.put(instance, topState);
    }
    preferredPartitionAssignment.put(partitions.get(0), replicaState);
    Map<String, Map<String, Map<String, String>>> preferredAssignment = new HashMap<>();
    preferredAssignment.put(resourceNames.get(0), preferredPartitionAssignment);

    // inject invalid partition assignment for one resources into preferred assignment.
    instances = instanceNames.subList(0, nReplicas - 1);
    Map<String, String> invalidReplicaState = new HashMap<>();
    for (String instance : instances) {
      invalidReplicaState.put(instance, topState);
    }
    preferredPartitionAssignment = new HashMap<>();
    preferredPartitionAssignment.put(partitions.get(0), invalidReplicaState);
    preferredAssignment.put(resourceNames.get(1), preferredPartitionAssignment);

    Map<String, Map<String, Map<String, String>>> assignment = new HashMap<>();
    ConstraintRebalanceStrategy strategy = new ConstraintRebalanceStrategy(Collections.EMPTY_LIST,
        Collections.<AbstractRebalanceSoftConstraint>singletonList(evenConstraint));
    for (String resourceName : resourceNames) {
      Map<String, Map<String, String>> partitionMap = new HashMap<>();

      strategy.init(resourceName, partitions, states, Integer.MAX_VALUE);
      partitionMap.putAll(strategy.computePartitionAssignment(instanceNames, instanceNames,
          preferredAssignment.containsKey(resourceName) ?
              preferredAssignment.get(resourceName) :
              Collections.EMPTY_MAP, cache).getMapFields());
      assignment.put(resourceName, partitionMap);
    }

    // Even with preferred assignment, the weight should still be balance
    Map<String, Integer> weightCount = checkPartitionUsage(assignment, weightProvider);
    int max = Collections.max(weightCount.values());
    int min = Collections.min(weightCount.values());
    // Since the accuracy of Default evenness constraint is 0.01, diff should be 1/100 of participant capacity in max.
    Assert.assertTrue((max - min) <= defaultCapacity / 100);

    // the resource 0 assignment should be kept the same
    Collection<String> resource_0_Assignment =
        assignment.get(resourceNames.get(0)).get(partitions.get(0)).keySet();
    Assert.assertTrue(resource_0_Assignment.containsAll(instanceNames.subList(0, nReplicas))
        && resource_0_Assignment.size() == nReplicas);
    // the resource 1 assignment should be set to a valid one
    Assert.assertTrue(
        assignment.get(resourceNames.get(1)).get(partitions.get(0)).size() == nReplicas);
  }

  @Test
  public void testTopologyAwareAssignment() {
    // Topology Aware configuration
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
    List<LiveInstance> liveInstanceList = new ArrayList<>();
    Map<String, InstanceConfig> instanceConfigs = new HashMap<>();
    for (int i = 0; i < instanceNames.size(); i++) {
      String instance = instanceNames.get(i);
      LiveInstance liveInstance = new LiveInstance(instance);
      liveInstanceList.add(liveInstance);
      InstanceConfig config = new InstanceConfig(instance);
      config.setDomain(String.format("Rack=%s,Host=%s", i % (nParticipants / 5), instance));
      instanceConfigs.put(instance, config);
    }
    cache.setLiveInstances(liveInstanceList);
    cache.setInstanceConfigMap(instanceConfigs);
    ClusterConfig clusterConfig = new ClusterConfig("test");
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/Rack/Host");
    clusterConfig.setFaultZoneType("Rack");
    cache.setClusterConfig(clusterConfig);

    Map<String, Map<String, Map<String, String>>> result = new HashMap<>();
    ConstraintRebalanceStrategy strategy = new ConstraintRebalanceStrategy();

    for (String resourceName : resourceNames) {
      Map<String, Map<String, String>> partitionMap = new HashMap<>();

      strategy.init(resourceName, partitions, states, Integer.MAX_VALUE);
      partitionMap.putAll(strategy.computePartitionAssignment(instanceNames, instanceNames,
          new HashMap<String, Map<String, String>>(), cache).getMapFields());
      result.put(resourceName, partitionMap);
    }

    Map<String, Integer> weightCount = checkPartitionUsage(result, new PartitionWeightProvider() {
      @Override
      public int getPartitionWeight(String resource, String partition) {
        return defaultCapacity;
      }
    });
    int max = Collections.max(weightCount.values());
    int min = Collections.min(weightCount.values());
    Assert.assertTrue((max - min) <= defaultCapacity / 100);

    // check for domain assignment
    Map<String, Set<String>> domainPartitionMap = new HashMap<>();
    for (Map<String, Map<String, String>> partitionMap : result.values()) {
      domainPartitionMap.clear();
      for (String partition : partitionMap.keySet()) {
        for (String instance : partitionMap.get(partition).keySet()) {
          String domain = instanceConfigs.get(instance).getDomain().split(",")[0].split("=")[1];
          if (domainPartitionMap.containsKey(domain)) {
            Assert.assertFalse(domainPartitionMap.get(domain).contains(partition));
          } else {
            domainPartitionMap.put(domain, new HashSet<String>());
          }
          domainPartitionMap.get(domain).add(partition);
        }
      }
    }
  }
}
