package org.apache.helix.integration;

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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.RebalanceConfig;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceHardConstraint;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceSoftConstraint;
import org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.rebalancer.constraint.PartitionWeightAwareEvennessConstraint;
import org.apache.helix.controller.rebalancer.constraint.TotalCapacityConstraint;
import org.apache.helix.controller.rebalancer.constraint.dataprovider.MockCapacityProvider;
import org.apache.helix.controller.rebalancer.constraint.dataprovider.MockPartitionWeightProvider;
import org.apache.helix.controller.rebalancer.constraint.dataprovider.ZkBasedCapacityProvider;
import org.apache.helix.controller.rebalancer.constraint.dataprovider.ZkBasedPartitionWeightProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.OnlineOfflineSMD;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.util.WeightAwareRebalanceUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.controller.rebalancer.constraint.dataprovider.ZkBasedPartitionWeightProvider.DEFAULT_WEIGHT_VALUE;

public class TestWeightBasedRebalanceUtil extends ZkTestBase {
  private static String CLUSTER_NAME;

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
  final List<ResourceConfig> resourceConfigs = new ArrayList<>();

  final LinkedHashMap<String, Integer> states = new LinkedHashMap<>(2);

  final ClusterConfig clusterConfig = new ClusterConfig(CLUSTER_NAME);
  final List<InstanceConfig> instanceConfigs = new ArrayList<>();

  @BeforeClass
  public void beforeClass() {
    System.out.println(
        "START " + getClass().getSimpleName() + " at " + new Date(System.currentTimeMillis()));

    CLUSTER_NAME = "MockCluster" + getShortClassName();

    for (int i = 0; i < nParticipants; i++) {
      instanceNames.add("node" + i);
    }
    for (int i = 0; i < nPartitions; i++) {
      partitions.add(Integer.toString(i));
    }

    for (int i = 0; i < nResources; i++) {
      resourceNames.add(resourceNamePrefix + i);
      ResourceConfig.Builder resourcBuilder = new ResourceConfig.Builder(resourceNamePrefix + i);
      resourcBuilder.setStateModelDefRef("OnlineOffline");
      resourcBuilder.setNumReplica(nReplicas);
      for (String partition : partitions) {
        resourcBuilder.setPreferenceList(partition, Collections.EMPTY_LIST);
      }
      resourceConfigs.add(resourcBuilder.build());
    }

    setupMockCluster();

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);
  }

  @AfterClass
  public void afterClass() {
    deleteCluster(CLUSTER_NAME);
  }

  private void setupMockCluster() {
    for (String instance : instanceNames) {
      InstanceConfig config = new InstanceConfig(instance);
      instanceConfigs.add(config);
    }

    states.put("OFFLINE", 0);
    states.put(topState, nReplicas);
  }

  private Map<String, Integer> checkPartitionUsage(ResourcesStateMap assignment,
      PartitionWeightProvider weightProvider) {
    Map<String, Integer> weightCount = new HashMap<>();
    for (String resource : assignment.resourceSet()) {
      PartitionStateMap partitionMap = assignment.getPartitionStateMap(resource);
      for (Partition partition : partitionMap.partitionSet()) {
        // check states
        Map<String, Integer> stateCount = new HashMap<>(states);
        Map<String, String> stateMap = partitionMap.getPartitionMap(partition);
        for (String state : stateMap.values()) {
          Assert.assertTrue(stateCount.containsKey(state));
          stateCount.put(state, stateCount.get(state) - 1);
        }
        for (int count : stateCount.values()) {
          Assert.assertEquals(count, 0);
        }

        // report weight
        int partitionWeight =
            weightProvider.getPartitionWeight(resource, partition.getPartitionName());
        for (String instance : partitionMap.getPartitionMap(partition).keySet()) {
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

  private void validateWeight(PartitionWeightProvider provider) {
    for (String resource : resourceNames) {
      for (String partition : partitions) {
        int weight = provider.getPartitionWeight(resource, partition);
        if (resource.equals(resourceNames.get(0))) {
          if (partition.equals(partitions.get(0))) {
            Assert.assertEquals(weight, resourceWeight * 3);
          } else {
            Assert.assertEquals(weight, resourceWeight * 2);
          }
        } else if (resource.equals(resourceNames.get(1))) {
          if (partition.equals(partitions.get(0))) {
            Assert.assertEquals(weight, resourceWeight * 3);
          } else {
            Assert.assertEquals(weight, resourceWeight);
          }
        } else {
          Assert.assertEquals(weight, resourceWeight);
        }
      }
    }
  }

  @Test
  public void testRebalance() {
    // capacity / weight
    Map<String, Integer> capacity = new HashMap<>();
    for (String instance : instanceNames) {
      capacity.put(instance, defaultCapacity);
    }

    MockCapacityProvider capacityProvider = new MockCapacityProvider(capacity, defaultCapacity);

    MockPartitionWeightProvider weightProvider = new MockPartitionWeightProvider(resourceWeight);

    TotalCapacityConstraint capacityConstraint =
        new TotalCapacityConstraint(weightProvider, capacityProvider);
    PartitionWeightAwareEvennessConstraint evenConstraint =
        new PartitionWeightAwareEvennessConstraint(weightProvider, capacityProvider);

    WeightAwareRebalanceUtil util = new WeightAwareRebalanceUtil(clusterConfig, instanceConfigs);
    ResourcesStateMap assignment = util.buildIncrementalRebalanceAssignment(resourceConfigs, null,
        Collections.<AbstractRebalanceHardConstraint> singletonList(capacityConstraint),
        Collections.<AbstractRebalanceSoftConstraint> singletonList(evenConstraint));
    Map<String, Integer> weightCount = checkPartitionUsage(assignment, weightProvider);

    int max = Collections.max(weightCount.values());
    int min = Collections.min(weightCount.values());
    // Since the accuracy of Default evenness constraint is 0.01, diff should be 1/100 of
    // participant capacity in max.
    Assert.assertTrue((max - min) <= defaultCapacity / 100);
  }

  @Test
  public void testZkBasedCapacityProvider() {
    Map<String, Integer> resourceDefaultWeightMap = new HashMap<>();
    resourceDefaultWeightMap.put(resourceNames.get(0), resourceWeight * 2);
    Map<String, Map<String, Integer>> partitionWeightMap = new HashMap<>();
    partitionWeightMap.put(resourceNames.get(0),
        Collections.singletonMap(partitions.get(0), resourceWeight * 3));
    partitionWeightMap.put(resourceNames.get(1),
        Collections.singletonMap(partitions.get(0), resourceWeight * 3));

    ZkBasedPartitionWeightProvider weightProvider =
        new ZkBasedPartitionWeightProvider(ZK_ADDR, CLUSTER_NAME, "Test");
    weightProvider.updateWeights(resourceDefaultWeightMap, partitionWeightMap, resourceWeight);
    // verify before persist
    validateWeight(weightProvider);

    // persist get values back
    weightProvider.persistWeights();
    // verify after persist
    weightProvider = new ZkBasedPartitionWeightProvider(ZK_ADDR, CLUSTER_NAME, "Test");
    validateWeight(weightProvider);

    weightProvider = new ZkBasedPartitionWeightProvider(ZK_ADDR, CLUSTER_NAME, "Fack");
    for (String resource : resourceNames) {
      for (String partition : partitions) {
        Assert.assertEquals(weightProvider.getPartitionWeight(resource, partition),
            DEFAULT_WEIGHT_VALUE);
      }
    }

    // update with invalid value
    weightProvider.updateWeights(Collections.EMPTY_MAP, Collections.EMPTY_MAP, -1);
    try {
      weightProvider.persistWeights();
      Assert.fail("Should fail to persist invalid weight information.");
    } catch (HelixException hex) {
      // expected
    }

    Map<String, Integer> capacity = new HashMap<>();
    Map<String, Integer> usage = new HashMap<>();
    for (int i = 0; i < instanceNames.size(); i++) {
      capacity.put(instanceNames.get(i), defaultCapacity + i);
      usage.put(instanceNames.get(i), i);
    }

    ZkBasedCapacityProvider capacityProvider =
        new ZkBasedCapacityProvider(ZK_ADDR, CLUSTER_NAME, "Test");
    capacityProvider.updateCapacity(capacity, usage, defaultCapacity);

    for (String instance : instanceNames) {
      Assert.assertEquals(capacityProvider.getParticipantCapacity(instance),
          capacity.get(instance).intValue());
      Assert.assertEquals(capacityProvider.getParticipantUsage(instance),
          usage.get(instance).intValue());
    }

    // persist get values back
    capacityProvider.persistCapacity();
    capacityProvider = new ZkBasedCapacityProvider(ZK_ADDR, CLUSTER_NAME, "Test");
    for (String instance : instanceNames) {
      Assert.assertEquals(capacityProvider.getParticipantCapacity(instance),
          capacity.get(instance).intValue());
      Assert.assertEquals(capacityProvider.getParticipantUsage(instance),
          usage.get(instance).intValue());
    }

    // update usage
    String targetInstanceName = instanceNames.get(0);
    int newUsgae = 12345;
    capacityProvider.updateCapacity(Collections.EMPTY_MAP,
        Collections.singletonMap(targetInstanceName, newUsgae), defaultCapacity);
    Assert.assertEquals(capacityProvider.getParticipantUsage(targetInstanceName), newUsgae);
    // check again without updating ZK
    capacityProvider = new ZkBasedCapacityProvider(ZK_ADDR, CLUSTER_NAME, "Test");
    Assert.assertEquals(capacityProvider.getParticipantUsage(targetInstanceName), 0);

    // update with invalid value
    capacityProvider.updateCapacity(Collections.EMPTY_MAP, Collections.EMPTY_MAP, -1);
    try {
      capacityProvider.persistCapacity();
      Assert.fail("Should fail to persist invalid weight information.");
    } catch (HelixException hex) {
      // expected
    }
  }

  @Test
  public void testRebalanceUsingZkDataProvider() {
    // capacity / weight
    Map<String, Integer> capacity = new HashMap<>();
    for (String instance : instanceNames) {
      capacity.put(instance, defaultCapacity);
    }

    ZkBasedPartitionWeightProvider weightProvider =
        new ZkBasedPartitionWeightProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    weightProvider.updateWeights(Collections.EMPTY_MAP, Collections.EMPTY_MAP, resourceWeight);

    ZkBasedCapacityProvider capacityProvider =
        new ZkBasedCapacityProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    capacityProvider.updateCapacity(capacity, Collections.EMPTY_MAP, 0);

    TotalCapacityConstraint capacityConstraint =
        new TotalCapacityConstraint(weightProvider, capacityProvider);
    PartitionWeightAwareEvennessConstraint evenConstraint =
        new PartitionWeightAwareEvennessConstraint(weightProvider, capacityProvider);

    WeightAwareRebalanceUtil util = new WeightAwareRebalanceUtil(clusterConfig, instanceConfigs);
    ResourcesStateMap assignment = util.buildIncrementalRebalanceAssignment(resourceConfigs, null,
        Collections.<AbstractRebalanceHardConstraint> singletonList(capacityConstraint),
        Collections.<AbstractRebalanceSoftConstraint> singletonList(evenConstraint));
    Map<String, Integer> weightCount = checkPartitionUsage(assignment, weightProvider);

    int max = Collections.max(weightCount.values());
    int min = Collections.min(weightCount.values());
    // Since the accuracy of Default evenness constraint is 0.01, diff should be 1/100 of
    // participant capacity in max.
    Assert.assertTrue((max - min) <= defaultCapacity / 100);
  }

  @Test(dependsOnMethods = "testRebalanceUsingZkDataProvider")
  public void testRebalanceWithExistingUsage() {
    // capacity / weight
    Map<String, Integer> capacity = new HashMap<>();
    Map<String, Integer> usage = new HashMap<>();
    for (int i = 0; i < instanceNames.size(); i++) {
      String instance = instanceNames.get(i);
      capacity.put(instance, defaultCapacity);
      if (i % 7 == 0) {
        usage.put(instance, defaultCapacity);
      }
    }

    ZkBasedPartitionWeightProvider weightProvider =
        new ZkBasedPartitionWeightProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    weightProvider.updateWeights(Collections.EMPTY_MAP, Collections.EMPTY_MAP, resourceWeight);

    ZkBasedCapacityProvider capacityProvider =
        new ZkBasedCapacityProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    capacityProvider.updateCapacity(capacity, usage, 0);

    TotalCapacityConstraint hardConstraint =
        new TotalCapacityConstraint(weightProvider, capacityProvider);
    PartitionWeightAwareEvennessConstraint evenConstraint =
        new PartitionWeightAwareEvennessConstraint(weightProvider, capacityProvider);

    WeightAwareRebalanceUtil util = new WeightAwareRebalanceUtil(clusterConfig, instanceConfigs);
    ResourcesStateMap assignment = util.buildIncrementalRebalanceAssignment(resourceConfigs, null,
        Collections.<AbstractRebalanceHardConstraint> singletonList(hardConstraint),
        Collections.<AbstractRebalanceSoftConstraint> singletonList(evenConstraint));
    Map<String, Integer> weightCount = checkPartitionUsage(assignment, weightProvider);

    for (int i = 0; i < instanceNames.size(); i++) {
      String instance = instanceNames.get(i);
      if (i % 7 == 0) {
        Assert.assertFalse(weightCount.containsKey(instance));
      } else {
        Assert.assertTrue(weightCount.get(instance) > 0);
      }
    }
  }

  @Test(dependsOnMethods = "testRebalanceUsingZkDataProvider")
  public void testRebalanceOption() {
    // capacity / weight
    Map<String, Integer> capacity = new HashMap<>();
    for (String instance : instanceNames) {
      capacity.put(instance, defaultCapacity);
    }

    ZkBasedPartitionWeightProvider weightProvider =
        new ZkBasedPartitionWeightProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    weightProvider.updateWeights(Collections.EMPTY_MAP, Collections.EMPTY_MAP, resourceWeight);

    ZkBasedCapacityProvider capacityProvider =
        new ZkBasedCapacityProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    capacityProvider.updateCapacity(capacity, Collections.EMPTY_MAP, 0);

    PartitionWeightAwareEvennessConstraint evenConstraint =
        new PartitionWeightAwareEvennessConstraint(weightProvider, capacityProvider);

    // Assume existing assignment
    ResourcesStateMap existingAssignment = new ResourcesStateMap();
    String targetResource = resourceNames.get(0);
    for (String partition : partitions) {
      for (int i = 0; i < nReplicas; i++) {
        existingAssignment.setState(targetResource, new Partition(partition), instanceNames.get(i),
            topState);
      }
    }

    WeightAwareRebalanceUtil util = new WeightAwareRebalanceUtil(clusterConfig, instanceConfigs);

    // INCREMENTAL
    ResourcesStateMap assignment = util.buildIncrementalRebalanceAssignment(resourceConfigs,
        existingAssignment, Collections.EMPTY_LIST,
        Collections.<AbstractRebalanceSoftConstraint> singletonList(evenConstraint));
    // check if the existingAssignment is changed
    for (String partition : partitions) {
      Assert.assertTrue(assignment.getInstanceStateMap(targetResource, new Partition(partition))
          .keySet().containsAll(instanceNames.subList(0, nReplicas)));
    }
    // still need to check for balance
    Map<String, Integer> weightCount = checkPartitionUsage(assignment, weightProvider);
    int max = Collections.max(weightCount.values());
    int min = Collections.min(weightCount.values());
    // Since the accuracy of Default evenness constraint is 0.01, diff should be 1/100 of
    // participant capacity in max.
    Assert.assertTrue((max - min) <= defaultCapacity / 100);

    // FULL
    assignment = util.buildFullRebalanceAssignment(resourceConfigs, existingAssignment,
        Collections.EMPTY_LIST,
        Collections.<AbstractRebalanceSoftConstraint> singletonList(evenConstraint));
    // check if the existingAssignment is changed
    for (String partition : partitions) {
      Assert.assertFalse(assignment.getInstanceStateMap(targetResource, new Partition(partition))
          .keySet().containsAll(instanceNames.subList(0, nReplicas)));
    }
  }

  @Test(dependsOnMethods = "testRebalanceUsingZkDataProvider")
  public void testInvalidInput() {
    // capacity / weight
    Map<String, Integer> capacity = new HashMap<>();
    for (String instance : instanceNames) {
      capacity.put(instance, defaultCapacity);
    }

    ZkBasedPartitionWeightProvider weightProvider =
        new ZkBasedPartitionWeightProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    weightProvider.updateWeights(Collections.EMPTY_MAP, Collections.EMPTY_MAP, resourceWeight);

    ZkBasedCapacityProvider capacityProvider =
        new ZkBasedCapacityProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    capacityProvider.updateCapacity(capacity, Collections.EMPTY_MAP, 0);

    TotalCapacityConstraint capacityConstraint =
        new TotalCapacityConstraint(weightProvider, capacityProvider);

    WeightAwareRebalanceUtil util = new WeightAwareRebalanceUtil(clusterConfig, instanceConfigs);

    // Empty constraint
    try {
      util.buildIncrementalRebalanceAssignment(resourceConfigs, null, Collections.EMPTY_LIST,
          Collections.EMPTY_LIST);
      Assert.fail("Should fail due to empty constraint list.");
    } catch (HelixException ex) {
      // expected
    }

    ResourceConfig.Builder invalidResourceBuilder = new ResourceConfig.Builder("InvalidResource");
    invalidResourceBuilder.setStateModelDefRef("OnlineOffline");
    invalidResourceBuilder.setNumPartitions(nPartitions);
    invalidResourceBuilder.setNumReplica(nReplicas);
    for (String partition : partitions) {
      invalidResourceBuilder.setPreferenceList(partition, Collections.EMPTY_LIST);
    }

    // Auto mode resource config
    try {
      invalidResourceBuilder
          .setRebalanceConfig(new RebalanceConfig(new ZNRecord("InvalidResource")));
      invalidResourceBuilder.getRebalanceConfig()
          .setRebalanceMode(RebalanceConfig.RebalanceMode.FULL_AUTO);
      util.buildIncrementalRebalanceAssignment(
          Collections.singletonList(invalidResourceBuilder.build()), null,
          Collections.<AbstractRebalanceHardConstraint> singletonList(capacityConstraint),
          Collections.EMPTY_LIST);
      Assert.fail("Should fail due to full auto resource config.");
    } catch (HelixException ex) {
      // expected
      invalidResourceBuilder.getRebalanceConfig()
          .setRebalanceMode(RebalanceConfig.RebalanceMode.CUSTOMIZED);
    }

    // Auto mode resource config
    try {
      invalidResourceBuilder.setStateModelDefRef("CustomizedOnlineOffline");
      util.buildIncrementalRebalanceAssignment(
          Collections.singletonList(invalidResourceBuilder.build()), null,
          Collections.<AbstractRebalanceHardConstraint> singletonList(capacityConstraint),
          Collections.EMPTY_LIST);
      Assert.fail("Should fail due to unknown state model def ref.");
    } catch (IllegalArgumentException ex) {
      // expected
      util.registerCustomizedStateModelDef("CustomizedOnlineOffline", OnlineOfflineSMD.build());
      ResourcesStateMap assignment = util.buildIncrementalRebalanceAssignment(
          Collections.singletonList(invalidResourceBuilder.build()), null,
          Collections.<AbstractRebalanceHardConstraint> singletonList(capacityConstraint),
          Collections.EMPTY_LIST);
      checkPartitionUsage(assignment, weightProvider);
    }
  }
}
