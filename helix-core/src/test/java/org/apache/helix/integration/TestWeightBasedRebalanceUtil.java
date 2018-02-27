package org.apache.helix.integration;

import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.RebalanceConfig;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceHardConstraint;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceSoftConstraint;
import org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionQuotaProvider;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.rebalancer.constraint.PartitionWeightAwareEvennessConstraint;
import org.apache.helix.controller.rebalancer.constraint.TotalCapacityConstraint;
import org.apache.helix.controller.rebalancer.constraint.dataprovider.ZkBasedCapacityProvider;
import org.apache.helix.controller.rebalancer.constraint.dataprovider.ZkBasedPartitionQuotaProvider;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.model.*;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.util.WeightAwareRebalanceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.*;

import static org.apache.helix.controller.rebalancer.constraint.dataprovider.ZkBasedPartitionQuotaProvider.DEFAULT_QUOTA_VALUE;

public class TestWeightBasedRebalanceUtil extends ZkIntegrationTestBase {
  private static Logger _logger = LoggerFactory.getLogger(TestWeightBasedRebalanceUtil.class);
  private static String CLUSTER_NAME;
  private static ClusterSetup _setupTool;

  final String resourceNamePrefix = "resource";
  final int nParticipants = 40;
  final int nResources = 20;
  final int nPartitions = 100;
  final int nReplicas = 3;
  final int defaultCapacity = 6000; // total = 6000*40 = 240000
  final int resourceQuota = 10; // total = 20*100*3*10 = 60000
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
      resourcBuilder.setNumPartitions(nPartitions);
      resourcBuilder.setNumReplica(nReplicas);
      for (String partition : partitions) {
        resourcBuilder.setPreferenceList(partition, Collections.EMPTY_LIST);
      }
      resourceConfigs.add(resourcBuilder.build());
    }

    setupMockCluster();

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(ZK_ADDR);

    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);
  }

  @AfterClass
  public void afterClass() {
    _setupTool.deleteCluster(CLUSTER_NAME);
  }

  private void setupMockCluster() {
    for (String instance : instanceNames) {
      InstanceConfig config = new InstanceConfig(instance);
      instanceConfigs.add(config);
    }
    clusterConfig.setTopologyAwareEnabled(false);

    states.put("OFFLINE", 0);
    states.put(topState, nReplicas);
  }

  private Map<String, Integer> checkPartitionUsage(ResourcesStateMap assignment,
      PartitionQuotaProvider quotaProvider) {
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
            quotaProvider.getPartitionQuota(resource, partition.getPartitionName());
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

  private void validateQuota(PartitionQuotaProvider provider) {
    for (String resource : resourceNames) {
      for (String partition : partitions) {
        int quota = provider.getPartitionQuota(resource, partition);
        if (resource.equals(resourceNames.get(0))) {
          if (partition.equals(partitions.get(0))) {
            Assert.assertEquals(quota, resourceQuota * 3);
          } else {
            Assert.assertEquals(quota, resourceQuota * 2);
          }
        } else if (resource.equals(resourceNames.get(1))) {
          if (partition.equals(partitions.get(0))) {
            Assert.assertEquals(quota, resourceQuota * 3);
          } else {
            Assert.assertEquals(quota, resourceQuota);
          }
        } else {
          Assert.assertEquals(quota, resourceQuota);
        }
      }
    }
  }

  @Test
  public void testZkBasedCapacityProvider() {
    Map<String, Integer> resourceDefaultQuotaMap = new HashMap<>();
    resourceDefaultQuotaMap.put(resourceNames.get(0), resourceQuota * 2);
    Map<String, Map<String, Integer>> partitionQuotaMap = new HashMap<>();
    partitionQuotaMap
        .put(resourceNames.get(0), Collections.singletonMap(partitions.get(0), resourceQuota * 3));
    partitionQuotaMap
        .put(resourceNames.get(1), Collections.singletonMap(partitions.get(0), resourceQuota * 3));

    ZkBasedPartitionQuotaProvider quotaProvider =
        new ZkBasedPartitionQuotaProvider(ZK_ADDR, CLUSTER_NAME, "Test");
    quotaProvider.updateQuotas(resourceDefaultQuotaMap, partitionQuotaMap, resourceQuota);
    // verify before persist
    validateQuota(quotaProvider);

    // persist get values back
    quotaProvider.persistQuotas();
    quotaProvider = new ZkBasedPartitionQuotaProvider(ZK_ADDR, CLUSTER_NAME, "Test");
    validateQuota(quotaProvider);

    quotaProvider = new ZkBasedPartitionQuotaProvider(ZK_ADDR, CLUSTER_NAME, "Fack");
    for (String resource : resourceNames) {
      for (String partition : partitions) {
        Assert.assertEquals(quotaProvider.getPartitionQuota(resource, partition),
            DEFAULT_QUOTA_VALUE);
      }
    }

    // update with invalid value
    quotaProvider.updateQuotas(Collections.EMPTY_MAP, Collections.EMPTY_MAP, -1);
    try {
      quotaProvider.persistQuotas();
      Assert.fail("Should fail to persist invalid quota information.");
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
      Assert.assertEquals(capacityProvider.getParticipantProvisioned(instance),
          usage.get(instance).intValue());
    }

    // persist get values back
    capacityProvider.persistCapacity();
    capacityProvider = new ZkBasedCapacityProvider(ZK_ADDR, CLUSTER_NAME, "Test");
    for (String instance : instanceNames) {
      Assert.assertEquals(capacityProvider.getParticipantCapacity(instance),
          capacity.get(instance).intValue());
      Assert.assertEquals(capacityProvider.getParticipantProvisioned(instance),
          usage.get(instance).intValue());
    }

    // update usage
    String targetInstanceName = instanceNames.get(0);
    int newUsgae = 12345;
    capacityProvider.updateCapacity(Collections.EMPTY_MAP,
        Collections.singletonMap(targetInstanceName, newUsgae), defaultCapacity);
    Assert.assertEquals(capacityProvider.getParticipantProvisioned(targetInstanceName), newUsgae);
    // check again without updating ZK
    capacityProvider = new ZkBasedCapacityProvider(ZK_ADDR, CLUSTER_NAME, "Test");
    Assert.assertEquals(capacityProvider.getParticipantProvisioned(targetInstanceName), 0);

    // update with invalid value
    capacityProvider.updateCapacity(Collections.EMPTY_MAP, Collections.EMPTY_MAP, -1);
    try {
      capacityProvider.persistCapacity();
      Assert.fail("Should fail to persist invalid quota information.");
    } catch (HelixException hex) {
      // expected
    }
  }

  @Test
  public void testRebalanceUsingTool() {
    // capacity / weight
    Map<String, Integer> capacity = new HashMap<>();
    for (String instance : instanceNames) {
      capacity.put(instance, defaultCapacity);
    }

    ZkBasedPartitionQuotaProvider quotaProvider =
        new ZkBasedPartitionQuotaProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    quotaProvider.updateQuotas(Collections.EMPTY_MAP, Collections.EMPTY_MAP, resourceQuota);

    ZkBasedCapacityProvider capacityProvider =
        new ZkBasedCapacityProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    capacityProvider.updateCapacity(capacity, Collections.EMPTY_MAP, 0);

    TotalCapacityConstraint capacityConstraint =
        new TotalCapacityConstraint(quotaProvider, capacityProvider);
    PartitionWeightAwareEvennessConstraint evenConstraint =
        new PartitionWeightAwareEvennessConstraint(quotaProvider, capacityProvider, 1);

    WeightAwareRebalanceUtil util = new WeightAwareRebalanceUtil(clusterConfig, instanceConfigs);
    ResourcesStateMap assignment = util.calculateAssignment(resourceConfigs, null,
        WeightAwareRebalanceUtil.RebalanceOption.INIT,
        Collections.<AbstractRebalanceHardConstraint>singletonList(capacityConstraint),
        Collections.<AbstractRebalanceSoftConstraint>singletonList(evenConstraint));
    Map<String, Integer> weightCount = checkPartitionUsage(assignment, quotaProvider);

    int max = Collections.max(weightCount.values());
    int min = Collections.min(weightCount.values());
    // Since the accuracy of Default evenness constraint is 0.01, diff should be 1/100 of participant capacity in max.
    Assert.assertTrue((max - min) <= defaultCapacity / 100);
  }

  @Test(dependsOnMethods = "testRebalanceUsingTool")
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

    ZkBasedPartitionQuotaProvider quotaProvider =
        new ZkBasedPartitionQuotaProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    quotaProvider.updateQuotas(Collections.EMPTY_MAP, Collections.EMPTY_MAP, resourceQuota);

    ZkBasedCapacityProvider capacityProvider =
        new ZkBasedCapacityProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    capacityProvider.updateCapacity(capacity, usage, 0);

    TotalCapacityConstraint hardConstraint =
        new TotalCapacityConstraint(quotaProvider, capacityProvider);
    PartitionWeightAwareEvennessConstraint evenConstraint =
        new PartitionWeightAwareEvennessConstraint(quotaProvider, capacityProvider, 1);

    WeightAwareRebalanceUtil util = new WeightAwareRebalanceUtil(clusterConfig, instanceConfigs);
    ResourcesStateMap assignment = util.calculateAssignment(resourceConfigs, null,
        WeightAwareRebalanceUtil.RebalanceOption.INIT,
        Collections.<AbstractRebalanceHardConstraint>singletonList(hardConstraint),
        Collections.<AbstractRebalanceSoftConstraint>singletonList(evenConstraint));
    Map<String, Integer> weightCount = checkPartitionUsage(assignment, quotaProvider);

    for (int i = 0; i < instanceNames.size(); i++) {
      String instance = instanceNames.get(i);
      if (i % 7 == 0) {
        Assert.assertTrue(!weightCount.containsKey(instance));
      } else {
        Assert.assertTrue(weightCount.get(instance) > 0);
      }
    }
  }

  @Test(dependsOnMethods = "testRebalanceUsingTool")
  public void testRebalanceMode() {
    // capacity / weight
    Map<String, Integer> capacity = new HashMap<>();
    for (String instance : instanceNames) {
      capacity.put(instance, defaultCapacity);
    }

    ZkBasedPartitionQuotaProvider quotaProvider =
        new ZkBasedPartitionQuotaProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    quotaProvider.updateQuotas(Collections.EMPTY_MAP, Collections.EMPTY_MAP, resourceQuota);

    ZkBasedCapacityProvider capacityProvider =
        new ZkBasedCapacityProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    capacityProvider.updateCapacity(capacity, Collections.EMPTY_MAP, 0);

    PartitionWeightAwareEvennessConstraint evenConstraint =
        new PartitionWeightAwareEvennessConstraint(quotaProvider, capacityProvider, 1);

    // Assume existing assignment
    ResourcesStateMap existingAssignment = new ResourcesStateMap();
    String targetResource = resourceNames.get(0);
    for (String partition : partitions) {
      for (int i = 0; i < nReplicas; i++) {
        existingAssignment
            .setState(targetResource, new Partition(partition), instanceNames.get(i), topState);
      }
    }

    WeightAwareRebalanceUtil util = new WeightAwareRebalanceUtil(clusterConfig, instanceConfigs);

    // INIT
    ResourcesStateMap assignment = util.calculateAssignment(resourceConfigs, existingAssignment,
        WeightAwareRebalanceUtil.RebalanceOption.INIT, Collections.EMPTY_LIST,
        Collections.<AbstractRebalanceSoftConstraint>singletonList(evenConstraint));
    // check if the existingAssignment is changed
    for (String partition : partitions) {
      Assert.assertTrue(
          assignment.getInstanceStateMap(targetResource, new Partition(partition)).keySet()
              .containsAll(instanceNames.subList(0, nReplicas)));
    }
    // still need to check for balance
    Map<String, Integer> weightCount = checkPartitionUsage(assignment, quotaProvider);
    int max = Collections.max(weightCount.values());
    int min = Collections.min(weightCount.values());
    // Since the accuracy of Default evenness constraint is 0.01, diff should be 1/100 of participant capacity in max.
    Assert.assertTrue((max - min) <= defaultCapacity / 100);

    // REASSIGN
    assignment = util.calculateAssignment(resourceConfigs, existingAssignment,
        WeightAwareRebalanceUtil.RebalanceOption.REASSIGN, Collections.EMPTY_LIST,
        Collections.<AbstractRebalanceSoftConstraint>singletonList(evenConstraint));
    // check if the existingAssignment is changed
    for (String partition : partitions) {
      Assert.assertFalse(
          assignment.getInstanceStateMap(targetResource, new Partition(partition)).keySet()
              .containsAll(instanceNames.subList(0, nReplicas)));
    }
  }

  @Test(dependsOnMethods = "testRebalanceUsingTool")
  public void testInvalidInput() {
    // capacity / weight
    Map<String, Integer> capacity = new HashMap<>();
    for (String instance : instanceNames) {
      capacity.put(instance, defaultCapacity);
    }

    ZkBasedPartitionQuotaProvider quotaProvider =
        new ZkBasedPartitionQuotaProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    quotaProvider.updateQuotas(Collections.EMPTY_MAP, Collections.EMPTY_MAP, resourceQuota);

    ZkBasedCapacityProvider capacityProvider =
        new ZkBasedCapacityProvider(ZK_ADDR, CLUSTER_NAME, "QPS");
    capacityProvider.updateCapacity(capacity, Collections.EMPTY_MAP, 0);

    TotalCapacityConstraint capacityConstraint =
        new TotalCapacityConstraint(quotaProvider, capacityProvider);

    WeightAwareRebalanceUtil util = new WeightAwareRebalanceUtil(clusterConfig, instanceConfigs);

    // Empty constraint
    try {
      util.calculateAssignment(resourceConfigs, null, WeightAwareRebalanceUtil.RebalanceOption.INIT,
          Collections.EMPTY_LIST, Collections.EMPTY_LIST);
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
      invalidResourceBuilder.setRebalanceConfig(new RebalanceConfig(new ZNRecord("InvalidResource")));
      invalidResourceBuilder.getRebalanceConfig().setRebalanceMode(RebalanceConfig.RebalanceMode.FULL_AUTO);
      util.calculateAssignment(Collections.singletonList(invalidResourceBuilder.build()), null,
              WeightAwareRebalanceUtil.RebalanceOption.INIT,
              Collections.<AbstractRebalanceHardConstraint>singletonList(capacityConstraint),
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
      util.calculateAssignment(Collections.singletonList(invalidResourceBuilder.build()), null,
              WeightAwareRebalanceUtil.RebalanceOption.INIT,
              Collections.<AbstractRebalanceHardConstraint>singletonList(capacityConstraint),
              Collections.EMPTY_LIST);
      Assert.fail("Should fail due to unknown state model def ref.");
    } catch (IllegalArgumentException ex) {
      // expected
      util.registerCustomizedStateModelDef("CustomizedOnlineOffline", OnlineOfflineSMD.build());
      ResourcesStateMap assignment =
          util.calculateAssignment(Collections.singletonList(invalidResourceBuilder.build()), null,
              WeightAwareRebalanceUtil.RebalanceOption.INIT,
              Collections.<AbstractRebalanceHardConstraint>singletonList(capacityConstraint),
              Collections.EMPTY_LIST);
      checkPartitionUsage(assignment, quotaProvider);
    }
  }
}
