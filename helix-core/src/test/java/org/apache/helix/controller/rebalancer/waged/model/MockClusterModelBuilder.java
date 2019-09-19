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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

import org.apache.helix.model.ResourceAssignment;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;


/**
 * The builder class for generating an instance of {@link ClusterModel}
 *
 * Example of usage:
 *  new MockClusterModelBuilder("TestCluster")
 *        .setZoneCount(5)
 *        .setInstanceCountPerZone(10)
 *        .setResourceCount(5)
 *        .setPartitionCountPerResource(100)
 *        .setInstanceCapacity(ImmutableMap.of("size", 100))
 *        .setStateModels({"Master", "Slave"})
 *        .setPartitionMaxUsage(ImmutableMap.of("size", 20))
 *        .setPartitionUsageSampleMethod(value -> (int) (new Random().nextGaussian() * Math.sqrt(maxUsage)))
 *        .build()
 *
 * **WARNING**
 * The builder class is intended for test only; It has made some assumptions merely to simplify the creation process:
 *  1. The zone, instance, resource, partition names cannot be customized; It's because name differences's impact on rebalance algorithm result is minor.
 *  2. It doesn't support fine grained flexible settings yet;
 *   - The instance capacity is equal; Assuming it's a homogeneous instances environment
 *   - The instances number of each zone is equal
 *   - The partitions number of each resource is equal
 *   - The sampling method of all partitions usage is equal
 *   - The max allowed partitions hosted per instance is equal
 *   - All resources' partitions share the same state models (e.g {"Master", "Slave", "Slave"}, 1 master and 2 slaves)
 */
public class MockClusterModelBuilder {
  private static final String ZONE_PREFIX = "ZONE_";
  private static final String INSTANCE_PREFIX = "INSTANCE_";
  private static final String RESOURCE_PREFIX = "RESOURCE_";
  private static final String PARTITION_PREFIX = "PARTITION_";

  private final String testClusterName;
  // these are default values and can be overridden by set methods
  private int zoneCount = 4;
  private int instanceCountPerZone = 10;
  private int maxPartitionsPerInstance = Integer.MAX_VALUE;
  private int resourceMaxPartitionsPerInstance = Integer.MAX_VALUE;
  // 10 resources * 10 partitions per resource = 100 total resources by default
  private int resourceCount = 10;
  private int partitionCountPerResource = 10;
  private String[] stateModels = {"Master", "Slave", "Slave"};
  private Map<String, Integer> instanceCapacity = ImmutableMap.of("size", 1000, "rcu", 200);
  // by default, on average, one instance can at least host 20 partitions
  private Map<String, Integer> partitionMaxUsage = ImmutableMap.of("size", 50, "rcu", 10);
  // by default the partition usage distribution is a uniform distribution between [0, maxUsage]
  private Function<Integer, Integer> partitionUsageSampleMethod =
      (maxUsage) -> (int) Math.round(new Random().nextDouble() * maxUsage);

  private Map<String, ResourceAssignment> baselineAssignment = Collections.emptyMap();
  private Map<String, ResourceAssignment> bestPossibleAssignment = Collections.emptyMap();

  public MockClusterModelBuilder(String testClusterName) {
    this.testClusterName = testClusterName;
  }

  public MockClusterModelBuilder setZoneCount(int zoneCount) {
    this.zoneCount = zoneCount;
    return this;
  }

  public MockClusterModelBuilder setInstanceCountPerZone(int instanceCountPerZone) {
    this.instanceCountPerZone = instanceCountPerZone;
    return this;
  }

  public MockClusterModelBuilder setStateModels(String[] stateModels) {
    this.stateModels = stateModels;
    return this;
  }

  public MockClusterModelBuilder setPartitionCountPerResource(int partitionCountPerResource) {
    this.partitionCountPerResource = partitionCountPerResource;
    return this;
  }

  public MockClusterModelBuilder setResourceCount(int resourceCount) {
    this.resourceCount = resourceCount;
    return this;
  }

  public MockClusterModelBuilder setInstanceCapacity(Map<String, Integer> instanceCapacity) {
    this.instanceCapacity = instanceCapacity;
    return this;
  }

  public MockClusterModelBuilder setPartitionMaxUsage(Map<String, Integer> partitionMaxUsage) {
    this.partitionMaxUsage = partitionMaxUsage;
    return this;
  }

  public MockClusterModelBuilder setMaxPartitionsPerInstance(int maxPartitionsPerInstance) {
    this.maxPartitionsPerInstance = maxPartitionsPerInstance;
    return this;
  }

  public MockClusterModelBuilder setBaselineAssignment(Map<String, ResourceAssignment> baselineAssignment) {
    this.baselineAssignment = baselineAssignment;
    return this;
  }

  public MockClusterModelBuilder setBestPossibleAssignment(Map<String, ResourceAssignment> bestPossibleAssignment) {
    this.bestPossibleAssignment = bestPossibleAssignment;
    return this;
  }

  public MockClusterModelBuilder setPartitionUsageSampleMethod(Function<Integer, Integer> partitionUsageSampleMethod) {
    this.partitionUsageSampleMethod = partitionUsageSampleMethod;
    return this;
  }

  public MockClusterModelBuilder setResourceMaxPartitionsPerInstance(int resourceMaxPartitionsPerInstance) {
    this.resourceMaxPartitionsPerInstance = resourceMaxPartitionsPerInstance;
    return this;
  }

  /**
   * keep the rest methods public static for some cases when we don't need the full cluster model data
   */

  public static List<String> createFaultZones(String zonePrefix, int zoneCount) {
    List<String> zones = new ArrayList<>();
    for (int i = 0; i < zoneCount; i++) {
      zones.add(zonePrefix + i);
    }
    return zones;
  }

  public static List<String> createResources(String resourcePrefix, int resourceCount) {
    List<String> resources = new ArrayList<>();
    for (int i = 0; i < resourceCount; i++) {
      resources.add(resourcePrefix + i);
    }
    return resources;
  }

  // instance: id, cluster, faultZone, instance, capacity
  public static List<AssignableNode> createInstances(String instanceNamePrefix, int instanceCount, String zone,
      Map<String, Integer> capacity, int maxPartitionsPerInstance) {
    List<AssignableNode> instances = new ArrayList<>();
    instanceNamePrefix = zone + "_" + instanceNamePrefix;
    for (int i = 0; i < instanceCount; i++) {
      AssignableNode instance = new AssignableNode.Builder(instanceNamePrefix + i).faultZone(zone)
          .maxCapacity(capacity)
          .maxPartition(maxPartitionsPerInstance)
          .build();
      instances.add(instance);
    }
    return instances;
  }

  public static List<AssignableReplica> createReplicas(String partitionNamePrefix, int partitionCount,
      String resourceName, Map<String, Integer> maxCapacityUsage, int resourceMaxPartitionsPerInstance,
      List<String> stateModels, Function<Integer, Integer> sampleFunction) {
    List<AssignableReplica> replicas = new ArrayList<>();
    partitionNamePrefix = resourceName + "_" + partitionNamePrefix;
    Collections.sort(stateModels);
    for (int i = 0; i < partitionCount; i++) {
      String state = "";
      int statePriority = -1;
      Map<String, Integer> usage = new HashMap<>();
      for (String stateModel : stateModels) {
        if (!stateModel.equals(state)) {
          state = stateModel;
          statePriority++;
          usage = Maps.transformValues(maxCapacityUsage, sampleFunction::apply);
        }
        AssignableReplica replica =
            new AssignableReplica.Builder(partitionNamePrefix + "_" + i, resourceName).resourceMaxPartitionsPerInstance(
                resourceMaxPartitionsPerInstance)
                .capacityUsage(usage)
                .replicaState(state)
                .statePriority(statePriority)
                .build();
        replicas.add(replica);
      }
    }

    return replicas;
  }

  /**
   * The build method will construct and return an instance of {@link ClusterModel}
   * **WARNING**
   * All the replicas in the ClusterModel are un-assigned!
   */
  public MockClusterModel build() {
    List<String> zones = createFaultZones(ZONE_PREFIX, zoneCount);
    List<AssignableNode> instances = new ArrayList<>();
    for (String zone : zones) {
      instances.addAll(
          createInstances(INSTANCE_PREFIX, instanceCountPerZone, zone, instanceCapacity, maxPartitionsPerInstance));
    }
    List<String> resources = createResources(RESOURCE_PREFIX, resourceCount);
    List<AssignableReplica> allReplicas = new ArrayList<>();
    for (String resource : resources) {
      allReplicas.addAll(createReplicas(PARTITION_PREFIX, partitionCountPerResource, resource, partitionMaxUsage,
          resourceMaxPartitionsPerInstance, Arrays.asList(stateModels), partitionUsageSampleMethod));
    }
    ClusterContext clusterContext =
        new ClusterContext(new HashSet<>(allReplicas), instances.size(), baselineAssignment, bestPossibleAssignment);
    return new MockClusterModel(clusterContext, new HashSet<>(allReplicas), new HashSet<>(instances));
  }

  //TODO: sometimes we'd like to reproduce the result and need the support of dumping the cluster model into external format (csv, json, etc)
  public void dump(String fileName) {

  }
}
