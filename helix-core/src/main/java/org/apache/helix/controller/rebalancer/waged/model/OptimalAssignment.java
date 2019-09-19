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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.helix.HelixException;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

/**
 * The data model represents the optimal assignment of N replicas assigned to M instances;
 * It's mostly used as the return parameter of an assignment calculation algorithm; If the algorithm
 * failed to find optimal assignment given the endeavor, the user could check the failure reasons.
 * Note that this class is not thread safe.
 */
public class OptimalAssignment {
  private Map<AssignableNode, List<AssignableReplica>> _optimalAssignment = new HashMap<>();
  private Map<AssignableReplica, Map<AssignableNode, List<String>>> _failedAssignments =
      new HashMap<>();

  /**
   * Update the OptimalAssignment instance with the existing assignment recorded in the input
   * cluster model.
   * @param clusterModel
   */
  public void updateAssignments(ClusterModel clusterModel) {
    _optimalAssignment.clear();
    clusterModel.getAssignableNodes().values().stream()
        .forEach(node -> _optimalAssignment.put(node, new ArrayList<>(node.getAssignedReplicas())));
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
    List<AssignableNode> instances = new ArrayList<>(_optimalAssignment.keySet());
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

  /**
   * Compare this round of calculated assignment compared to the last assignment and get the
   * partition movements count
   * @param baseAssignment The base assignment (could be best possible/baseline assignment)
   * @return a simple cumulative count of total movements where differences of movements in terms of
   *         location, size, etc is ignored
   */
  public int getTotalPartitionMovements(Map<String, ResourceAssignment> baseAssignment) {
    Map<String, ResourceAssignment> optimalAssignment = getOptimalResourceAssignment();
    int movements = 0;
    for (String resource : optimalAssignment.keySet()) {
      final ResourceAssignment resourceAssignment = optimalAssignment.get(resource);
      if (!baseAssignment.containsKey(resource)) {
        // It means the resource is a newly added resource
        movements += resourceAssignment.getMappedPartitions().stream()
            .map(resourceAssignment::getReplicaMap).map(Map::size).count();
      } else {
        ResourceAssignment lastResourceAssignment = baseAssignment.get(resource);
        for (Partition partition : resourceAssignment.getMappedPartitions()) {
          Map<String, String> thisInstanceToStates = new HashMap<>(resourceAssignment.getReplicaMap(partition));
          Map<String, String> lastInstanceToStates = new HashMap<>(lastResourceAssignment.getReplicaMap(partition));
          MapDifference<String, String> diff = Maps.difference(thisInstanceToStates, lastInstanceToStates);
          //common keys(instances) but have different values (states)
          movements += diff.entriesDiffering().size();
          // Moved to different instances
          movements += diff.entriesOnlyOnLeft().size();
        }
      }
    }
    return movements;
  }

  /**
   * @return The optimal assignment in the form of a <Resource Name, ResourceAssignment> map.
   */
  public Map<String, ResourceAssignment> getOptimalResourceAssignment() {
    if (hasAnyFailure()) {
      throw new HelixException(
          "Cannot get the optimal resource assignment since a calculation failure is recorded. "
              + getErrorMessage());
    }
    Map<String, ResourceAssignment> assignmentMap = new HashMap<>();
    for (AssignableNode node : _optimalAssignment.keySet()) {
      for (AssignableReplica replica : _optimalAssignment.get(node)) {
        String resourceName = replica.getResourceName();
        Partition partition = new Partition(replica.getPartitionName());
        ResourceAssignment resourceAssignment = assignmentMap.computeIfAbsent(resourceName,
            key -> new ResourceAssignment(resourceName));
        Map<String, String> partitionStateMap = resourceAssignment.getReplicaMap(partition);
        if (partitionStateMap.isEmpty()) {
          // ResourceAssignment returns immutable empty map while no such assignment recorded yet.
          // So if the returned map is empty, create a new map.
          partitionStateMap = new HashMap<>();
        }
        partitionStateMap.put(node.getInstanceName(), replica.getReplicaState());
        resourceAssignment.addReplicaMap(partition, partitionStateMap);
      }
    }
    return assignmentMap;
  }

  public void recordAssignmentFailure(AssignableReplica replica,
      Map<AssignableNode, List<String>> failedReasons) {
    _failedAssignments.put(replica, failedReasons);
  }

  public boolean hasAnyFailure() {
    return !_failedAssignments.isEmpty();
  }

  public String getErrorMessage() {
    StringBuilder errorMessageBuilder = new StringBuilder();
    for (AssignableReplica replica : _failedAssignments.keySet()) {
      String partitionName = replica.toString();
      errorMessageBuilder.append(partitionName).append("\n");
      Map<AssignableNode, List<String>> failedAssignments = _failedAssignments.get(replica);
      failedAssignments.entrySet().forEach(entry -> {
        errorMessageBuilder.append("\t").append(entry.getKey().getInstanceName()).append(":")
            .append(
                String.join(",", entry.getValue().stream().sorted().collect(Collectors.toList())))
            .append("\n");
      });
    }
    return errorMessageBuilder.toString();
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
