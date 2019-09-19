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

import static com.google.common.math.DoubleMath.mean;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

public class MockClusterModel extends ClusterModel {
  public MockClusterModel(ClusterContext clusterContext, Set<AssignableReplica> unAssignedReplicas,
      Set<AssignableNode> assignableNodes) {
    super(clusterContext, unAssignedReplicas, assignableNodes);
  }

  public List<AssignableReplica> onInstanceCrash(String instanceName) {
    if (!getAssignableNodes().containsKey(instanceName)) {
      return Collections.emptyList();
    }
    AssignableNode assignableNode = getAssignableNodes().get(instanceName);
    Set<AssignableReplica> replicas = assignableNode.getAssignedReplicas();
    release(assignableNode, replicas);

    return new ArrayList<>(replicas);
  }

  public List<AssignableReplica> onInstanceAdditon(AssignableNode assignableNode) {
    // release everything
    getAssignableNodes().values().forEach(node -> release(node, node.getAssignedReplicas()));
    // add the new node
    getAssignableNodes().put(assignableNode.getInstanceName(), assignableNode);
    return new ArrayList<>(getContext().getAllReplicas());
  }

  //TODO: implement the remaining method
  public List<AssignableReplica> onResourceDeletion(String resource) {
    return Collections.emptyList();
  }

  public void onNewReplicasAddition(List<AssignableReplica> replicas) {

  }

  public void onReplicaWeightChange(AssignableReplica updatedReplica) {

  }

  public void onInstanceWeightChange(AssignableNode updatedNode) {

  }

  private void release(AssignableNode node, Collection<AssignableReplica> replicas) {
    for (AssignableReplica replica : replicas) {
      this.release(replica.getResourceName(), replica.getPartitionName(), replica.getReplicaState(), node.getInstanceName());
    }
  }

  /**
   * Compare the calculated assignment compared to the base assignment and get the
   * partition movements count
   * @param baseAssignment The base assignment (could be best possible/baseline assignment)
   * @return a simple cumulative count of total movements where differences of movements in terms of
   *         location, size, etc is ignored
   */
  public int getTotalMovedPartitionsCount(OptimalAssignment optimalAssignment,
      Map<String, ResourceAssignment> baseAssignment) {
    Map<String, ResourceAssignment> assignment = optimalAssignment.getOptimalResourceAssignment();
    int movements = 0;
    for (String resource : assignment.keySet()) {
      final ResourceAssignment resourceAssignment = assignment.get(resource);
      if (!baseAssignment.containsKey(resource)) {
        // It means the resource is a newly added resource
        movements += resourceAssignment.getMappedPartitions().stream()
            .map(resourceAssignment::getReplicaMap).map(Map::size).count();
      } else {
        ResourceAssignment lastResourceAssignment = baseAssignment.get(resource);
        for (Partition partition : resourceAssignment.getMappedPartitions()) {
          Map<String, String> thisInstanceToStates = resourceAssignment.getReplicaMap(partition);
          Map<String, String> lastInstanceToStates =
              lastResourceAssignment.getReplicaMap(partition);
          MapDifference<String, String> diff =
              Maps.difference(thisInstanceToStates, lastInstanceToStates);
          // common keys(instances) but have different values (states)
          movements += diff.entriesDiffering().size();
          // Moved to different instances
          movements += diff.entriesOnlyOnLeft().size();
        }
      }
    }
    return movements;
  }

  /**
   * The coefficient of variation (CV) is a statistical measure of the dispersion of data points in
   * a data series around the mean.
   * The coefficient of variation represents the ratio of the standard deviation to the mean, and it
   * is a useful statistic for comparing the degree of variation from one data series to another,
   * even if the means are drastically different from one another.
   * It's used as a tool to evaluate the "evenness" of an partitions to instances assignment
   * @return a multi-dimension CV keyed by capacity key
   */
  public Map<String, Double> getCoefficientOfVariationAsEvenness() {
    List<AssignableNode> instances = new ArrayList<>(getAssignableNodes().values());
    Map<String, List<Integer>> usages = new HashMap<>();
    for (AssignableNode instance : instances) {
      Map<String, Integer> capacityUsage = instance.getCapacityUsage();
      for (String key : capacityUsage.keySet()) {
        usages.computeIfAbsent(key, k -> new ArrayList<>()).add(capacityUsage.get(key));
      }
    }

    return usages.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> getCoefficientOfVariation(e.getValue())));
  }

  private static double getCoefficientOfVariation(List<Integer> nums) {
    int sum = 0;
    double mean = mean(nums);
    for (int num : nums) {
      sum += Math.pow((num - mean), 2);
    }
    double std = Math.sqrt(sum / (nums.size() - 1));
    return std / mean;
  }
}
