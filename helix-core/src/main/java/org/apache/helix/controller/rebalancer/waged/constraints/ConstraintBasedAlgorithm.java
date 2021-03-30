package org.apache.helix.controller.rebalancer.waged.constraints;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ResourceAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The algorithm is based on a given set of constraints
 * - HardConstraint: Approve or deny the assignment given its condition, any assignment cannot
 * bypass any "hard constraint"
 * - SoftConstraint: Evaluate the assignment by points/rewards/scores, a higher point means a better
 * assignment
 * The goal is to accumulate the most points(rewards) from "soft constraints" while avoiding any
 * "hard constraints"
 */
class ConstraintBasedAlgorithm implements RebalanceAlgorithm {
  private static final Logger LOG = LoggerFactory.getLogger(ConstraintBasedAlgorithm.class);
  private final List<HardConstraint> _hardConstraints;
  private final Map<SoftConstraint, Float> _softConstraints;


  ConstraintBasedAlgorithm(List<HardConstraint> hardConstraints,
      Map<SoftConstraint, Float> softConstraints) {
    _hardConstraints = hardConstraints;
    _softConstraints = softConstraints;
  }

  @Override
  public OptimalAssignment calculate(ClusterModel clusterModel) throws HelixRebalanceException {
    OptimalAssignment optimalAssignment = new OptimalAssignment();
    List<AssignableNode> nodes = new ArrayList<>(clusterModel.getAssignableNodes().values());
    Set<String> busyInstances =
        getBusyInstances(clusterModel.getContext().getBestPossibleAssignment().values());
    List<AssignableReplica> toBeAssignedReplicas = clusterModel.getAssignableReplicaMap().values().stream()
        .flatMap(replicas -> replicas.stream()).collect(Collectors.toList());
    // Compute overall utilization of the cluster. Capacity dimension -> <total usage, total capacity>
    Map<String, MutablePair<Integer, Integer>> overallClusterUtilMap =
        computeOverallClusterUtil(nodes);
    Map<String, Integer> replicaHashCodeMap = toBeAssignedReplicas.parallelStream().collect(Collectors
        .toMap(AssignableReplica::toString,
            replica -> Objects.hash(replica.toString(), clusterModel.getAssignableNodes().keySet()),
            (hash1, hash2) -> hash2));
    Pair<AssignableReplica, List<AssignableReplica>> replicaRes =
        getNextAssignableReplica(toBeAssignedReplicas, overallClusterUtilMap, clusterModel,
            replicaHashCodeMap);
    AssignableReplica replica = replicaRes.getLeft();
    toBeAssignedReplicas = replicaRes.getRight();

    while (replica != null) {
      Optional<AssignableNode> maybeBestNode =
          getNodeWithHighestPoints(replica, nodes, clusterModel.getContext(), busyInstances,
              optimalAssignment);
      // stop immediately if any replica cannot find best assignable node
      if (!maybeBestNode.isPresent() || optimalAssignment.hasAnyFailure()) {
        String errorMessage = String.format(
            "Unable to find any available candidate node for partition %s; Fail reasons: %s",
            replica.getPartitionName(), optimalAssignment.getFailures());
        System.out.println(String.format(
            "Unable to find any available candidate node for partition %s; Fail reasons: %s",
            replica.getPartitionName(), optimalAssignment.getFailures()));
        throw new HelixRebalanceException(errorMessage,
            HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      }
      AssignableNode bestNode = maybeBestNode.get();
      // Assign the replica and update the cluster model.
      clusterModel
          .assign(replica.getResourceName(), replica.getPartitionName(), replica.getReplicaState(),
              bestNode.getInstanceName());
      updateOverallClusterUtil(overallClusterUtilMap, replica);
      // Compute next best replica and update remaining list.
      replicaRes = getNextAssignableReplica(toBeAssignedReplicas, overallClusterUtilMap, clusterModel,
          replicaHashCodeMap);
      replica = replicaRes.getLeft();
      toBeAssignedReplicas = replicaRes.getRight();
    }
    optimalAssignment.updateAssignments(clusterModel);
    return optimalAssignment;
  }

  private Optional<AssignableNode> getNodeWithHighestPoints(AssignableReplica replica,
      List<AssignableNode> assignableNodes, ClusterContext clusterContext,
      Set<String> busyInstances, OptimalAssignment optimalAssignment) {
    Map<AssignableNode, List<HardConstraint>> hardConstraintFailures = new ConcurrentHashMap<>();
    List<AssignableNode> candidateNodes = assignableNodes.parallelStream().filter(candidateNode -> {
      boolean isValid = true;
      // need to record all the failure reasons and it gives us the ability to debug/fix the runtime
      // cluster environment
      for (HardConstraint hardConstraint : _hardConstraints) {
        if (!hardConstraint.isAssignmentValid(candidateNode, replica, clusterContext)) {
          hardConstraintFailures.computeIfAbsent(candidateNode, node -> new ArrayList<>())
              .add(hardConstraint);
          isValid = false;
        }
      }
      return isValid;
    }).collect(Collectors.toList());

    if (candidateNodes.isEmpty()) {
      optimalAssignment.recordAssignmentFailure(replica,
          Maps.transformValues(hardConstraintFailures, this::convertFailureReasons));
      return Optional.empty();
    }

    return candidateNodes.parallelStream().map(node -> new HashMap.SimpleEntry<>(node,
        getAssignmentNormalizedScore(node, replica, clusterContext)))
        .max((nodeEntry1, nodeEntry2) -> {
          int scoreCompareResult = nodeEntry1.getValue().compareTo(nodeEntry2.getValue());
          if (scoreCompareResult == 0) {
            // If the evaluation scores of 2 nodes are the same, the algorithm assigns the replica
            // to the idle node first.
            String instanceName1 = nodeEntry1.getKey().getInstanceName();
            String instanceName2 = nodeEntry2.getKey().getInstanceName();
            int idleScore1 = busyInstances.contains(instanceName1) ? 0 : 1;
            int idleScore2 = busyInstances.contains(instanceName2) ? 0 : 1;
            return idleScore1 != idleScore2 ? (idleScore1 - idleScore2)
                : - instanceName1.compareTo(instanceName2);
          } else {
            return scoreCompareResult;
          }
        }).map(Map.Entry::getKey);
  }

  private double getAssignmentNormalizedScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    double sum = 0;
    for (Map.Entry<SoftConstraint, Float> softConstraintEntry : _softConstraints.entrySet()) {
      SoftConstraint softConstraint = softConstraintEntry.getKey();
      float weight = softConstraintEntry.getValue();
      if (weight != 0) {
        // Skip calculating zero weighted constraints.
        sum += weight * softConstraint.getAssignmentNormalizedScore(node, replica, clusterContext);
      }
    }
    return sum;
  }

  private List<String> convertFailureReasons(List<HardConstraint> hardConstraints) {
    return hardConstraints.stream().map(HardConstraint::getDescription)
        .collect(Collectors.toList());
  }

  private Map<String, MutablePair<Integer, Integer>> computeOverallClusterUtil(
      List<AssignableNode> nodes) {
    Map<String, MutablePair<Integer, Integer>> utilizationMap = new HashMap<>();
    for (AssignableNode node : nodes) {
      for (String resourceKey : node.getMaxCapacity().keySet()) {

        if (utilizationMap.containsKey(resourceKey)) {
          Integer newUtil = utilizationMap.get(resourceKey).getLeft() + node.getRemainingCapacity()
              .get(resourceKey);
          Integer newCapacity =
              utilizationMap.get(resourceKey).getLeft() + node.getMaxCapacity().get(resourceKey);
          // update util
          utilizationMap.get(resourceKey).setLeft(newUtil);
          // update capacity
          utilizationMap.get(resourceKey).setRight(newCapacity);
        } else {
          Integer newUtil = node.getRemainingCapacity().get(resourceKey);
          Integer newCapacity = node.getMaxCapacity().get(resourceKey);
          utilizationMap.put(resourceKey, new MutablePair<>(newUtil, newCapacity));
        }
      }
    }
    return utilizationMap;
  }

  /**
   * Update the overallClusterUtilMap with newly placed replica
   */
  private void updateOverallClusterUtil(
      Map<String, MutablePair<Integer, Integer>> overallClusterUtilMap, AssignableReplica replica) {
    for (Map.Entry<String, Integer> resourceUsage : replica.getCapacity().entrySet()) {
      Integer newUtil =
          overallClusterUtilMap.get(resourceUsage.getKey()).getLeft() - resourceUsage.getValue();
      overallClusterUtilMap.get(resourceUsage.getKey()).setLeft(newUtil);
    }
  }

  private Pair<AssignableReplica, List<AssignableReplica>> getNextAssignableReplica(
      List<AssignableReplica> allReplica,
      Map<String, MutablePair<Integer, Integer>> overallClusterUtilMap, ClusterModel clusterModel,
      Map<String, Integer> _replicaHashCodeMap) {
    List<AssignableReplica> replicaForNextRound = new ArrayList<>();
    float highestScore = -1;
    AssignableReplica nextAssinableReplica = null;
    Map<String, Float> weightMap = new HashMap<>();
    // compute weight for each capacity dimension
    for (Map.Entry<String, MutablePair<Integer, Integer>> resourceUsage : overallClusterUtilMap
        .entrySet()) {
      float freePercentage =
          (float) (resourceUsage.getValue().right - resourceUsage.getValue().left) / resourceUsage
              .getValue().right;
      weightMap.put(resourceUsage.getKey(), freePercentage);
    }
    // Compare every replica with current candidate, update candidate if needed
    for (AssignableReplica replica : allReplica) {
      // score = SUM(weight * Resource_capacity) where weight = 1/(1-resource_util)
      float score = 0.01f;
      for (Map.Entry<String, Integer> resourceCapacity : replica.getCapacity().entrySet()) {
        // use int to store % * 100
        int replicaUsageInClusterPoolPct =
            (resourceCapacity.getValue() * 100) / overallClusterUtilMap
                .get(resourceCapacity.getKey()).right;
        score += replicaUsageInClusterPoolPct / (weightMap.get(resourceCapacity.getKey()) + 0.01);
      }
      if (nextAssinableReplica == null) {
        nextAssinableReplica = replica;
        highestScore = score;
      } else if (!keepOriginalPreferredReplica(nextAssinableReplica, replica, clusterModel,
          _replicaHashCodeMap, highestScore, score)) {
        highestScore = score;
        if (nextAssinableReplica != null) {
          replicaForNextRound.add(nextAssinableReplica);
        }
        nextAssinableReplica = replica;
      } else {
        replicaForNextRound.add(replica);
      }
    }
    return new MutablePair<>(nextAssinableReplica, replicaForNextRound);
  }

  // return true is replica1 is better
  private boolean keepOriginalPreferredReplica(AssignableReplica replica1,
      AssignableReplica replica2, ClusterModel clusterModel,
      Map<String, Integer> replicaHashCodeMap, float replica1Score, float replica2Score) {
    Map<String, ResourceAssignment> _bestPossibleAssignment =
        clusterModel.getContext().getBestPossibleAssignment();
    Map<String, ResourceAssignment> _baselineAssignment =
        clusterModel.getContext().getBaselineAssignment();

    String resourceName1 = replica1.getResourceName();
    String resourceName2 = replica2.getResourceName();

    // 1. Sort according if the assignment exists in the best possible and/or baseline assignment
    if (_bestPossibleAssignment.containsKey(resourceName1) != _bestPossibleAssignment
        .containsKey(resourceName2)) {
      // If the best possible assignment contains only one replica's assignment,
      // prioritize the replica.
      return _bestPossibleAssignment.containsKey(resourceName1);
    }

    if (_baselineAssignment.containsKey(resourceName1) != _baselineAssignment
        .containsKey(resourceName2)) {
      // If the baseline assignment contains only one replica's assignment, prioritize the replica.
      return _baselineAssignment.containsKey(resourceName1);
    }

    // 2. Sort according to the state priority. Or the greedy algorithm will unnecessarily shuffle
    // the states between replicas.
    int statePriority1 = replica1.getStatePriority();
    int statePriority2 = replica2.getStatePriority();
    if (statePriority1 != statePriority2) {
      // Note we shall prioritize the replica with a higher state priority,
      // the smaller priority number means higher priority.
      return statePriority1 - statePriority2 < 0;
    }

    // 3. Sort according to the replica impact based on the weight.
    // So the greedy algorithm will place the more impactful replicas first.
    if (Math.abs(replica2Score - replica1Score) > 0.001f) {
      return replica1Score > replica2Score;
    }

    // 4. Sort according to the resource/partition name.
    // If none of the above conditions is making a difference, try to randomize the replicas
    // order.
    // Otherwise, the same replicas might always be moved in each rebalancing. This is because
    // their placement calculating will always happen at the critical moment while the cluster is
    // almost close to the expected utilization.
    //
    // Note that to ensure the algorithm is deterministic with the same inputs, do not use
    // Random functions here. Use hashcode based on the cluster topology information to get
    // a controlled randomized order is good enough.
    Integer replicaHash1 = replicaHashCodeMap.get(replica1.toString());
    Integer replicaHash2 = replicaHashCodeMap.get(replica2.toString());
    if (!replicaHash1.equals(replicaHash2)) {
      return replicaHash1.compareTo(replicaHash2) < 0;
    } else {
      // In case of hash collision, return order according to the name.
      return replica1.toString().compareTo(replica2.toString()) < 0;
    }
  }

  /**
   * @param assignments A collection of resource replicas assignment.
   * @return A set of instance names that have at least one replica assigned in the input assignments.
   */
  private Set<String> getBusyInstances(Collection<ResourceAssignment> assignments) {
    return assignments.stream().flatMap(
        resourceAssignment -> resourceAssignment.getRecord().getMapFields().values().stream()
            .flatMap(instanceStateMap -> instanceStateMap.keySet().stream())
            .collect(Collectors.toSet()).stream()).collect(Collectors.toSet());
  }
}
