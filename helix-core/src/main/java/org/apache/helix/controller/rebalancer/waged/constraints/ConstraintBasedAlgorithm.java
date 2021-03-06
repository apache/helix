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

    // Compute overall utilization of the cluster. Capacity dimension -> total remaining capacity
    Map<String, Integer> overallClusterRemainingCapacityMap =
        computeOverallClusterRemainingCapacity(nodes);

    // Create a wrapper for each AssignableReplica.
    Set<AssignableReplicaWithScore> toBeAssignedReplicas =
        clusterModel.getAssignableReplicaMap().values().stream()
            .flatMap(replicas -> replicas.stream())
            .map(replica -> new AssignableReplicaWithScore(replica, clusterModel))
            .collect(Collectors.toSet());

    while (!toBeAssignedReplicas.isEmpty()) {
      AssignableReplica replica =
          getNextAssignableReplica(toBeAssignedReplicas, overallClusterRemainingCapacityMap);
      Optional<AssignableNode> maybeBestNode =
          getNodeWithHighestPoints(replica, nodes, clusterModel.getContext(), busyInstances,
              optimalAssignment);
      // stop immediately if any replica cannot find best assignable node
      if (!maybeBestNode.isPresent() || optimalAssignment.hasAnyFailure()) {
        String errorMessage = String.format(
            "Unable to find any available candidate node for partition %s; Fail reasons: %s",
            replica.getPartitionName(), optimalAssignment.getFailures());
        throw new HelixRebalanceException(errorMessage,
            HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      }
      AssignableNode bestNode = maybeBestNode.get();
      // Assign the replica and update the cluster model.
      clusterModel
          .assign(replica.getResourceName(), replica.getPartitionName(), replica.getReplicaState(),
              bestNode.getInstanceName());
      updateOverallClusterRemainingCapacity(overallClusterRemainingCapacityMap, replica);
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

  private Map<String, Integer> computeOverallClusterRemainingCapacity(List<AssignableNode> nodes) {
    Map<String, Integer> utilizationMap = new HashMap<>();
    for (AssignableNode node : nodes) {
      for (String capacityKey : node.getMaxCapacity().keySet()) {
        utilizationMap.compute(capacityKey,
            (k, v) -> v == null ? node.getRemainingCapacity().get(capacityKey)
                : v + node.getRemainingCapacity().get(capacityKey));
      }
    }
    return utilizationMap;
  }

  /**
   * Update the overallClusterUtilMap with newly placed replica
   */
  private void updateOverallClusterRemainingCapacity(
      Map<String, Integer> overallClusterRemainingCapacityMap, AssignableReplica replica) {
    for (Map.Entry<String, Integer> resourceUsage : replica.getCapacity().entrySet()) {
      overallClusterRemainingCapacityMap.put(resourceUsage.getKey(),
          overallClusterRemainingCapacityMap.get(resourceUsage.getKey()) - resourceUsage
              .getValue());
    }
  }

  private class AssignableReplicaWithScore implements Comparable<AssignableReplicaWithScore> {
    private final AssignableReplica _replica;
    private float _score = 0;
    private final boolean _isInBestPossibleAssignment;
    private final boolean _isInBaselineAssignment;
    private final Integer  _replicaHash;

    AssignableReplicaWithScore(AssignableReplica replica, ClusterModel clusterModel) {
      _replica = replica;
      _isInBestPossibleAssignment = clusterModel.getContext().getBestPossibleAssignment()
          .containsKey(replica.getResourceName());
      _isInBaselineAssignment =
          clusterModel.getContext().getBaselineAssignment().containsKey(replica.getResourceName());
      _replicaHash = Objects.hash(replica.toString(), clusterModel.getAssignableNodes().keySet());
    }

    public void computeScore(Map<String, Integer> overallClusterRemainingCapMap) {
      float score = 0;
      // score = SUM(weight * (resource_capacity/cluster_capacity) where weight = 1/(1-total_util%)
      // it could be be simplified to "resource_capacity/cluster_remainingCapacity".
      for (Map.Entry<String, Integer> resourceCapacity : _replica.getCapacity().entrySet()) {
        if (resourceCapacity.getValue() == 0) {
          continue;
        }
        score = (overallClusterRemainingCapMap.get(resourceCapacity.getKey()) == 0
            || resourceCapacity.getValue() > (overallClusterRemainingCapMap
            .get(resourceCapacity.getKey()))) ? Float.MAX_VALUE
            : score + (float) resourceCapacity.getValue() / (overallClusterRemainingCapMap
                .get(resourceCapacity.getKey()));
        if (Float.compare(score, Float.MAX_VALUE) == 0) {
          break;
        }
      }
      _score = score;
    }

    public AssignableReplica getAssignableReplica() {
      return _replica;
    }


    @Override
    public String toString() {
      return _replica.toString();
    }

    @Override
    public int compareTo(AssignableReplicaWithScore replica2) {
      // 1. Sort according if the assignment exists in the best possible and/or baseline assignment
      if (_isInBestPossibleAssignment != replica2._isInBestPossibleAssignment) {
        // If the best possible assignment contains only one replica's assignment,
        // prioritize the replica.
        return _isInBestPossibleAssignment ? -1 : 1;
      }

      if (_isInBaselineAssignment != replica2._isInBaselineAssignment) {
        // If the baseline assignment contains only one replica's assignment, prioritize the replica.
        return _isInBaselineAssignment ? -1 : 1;
      }

      // 2. Sort according to the state priority. Or the greedy algorithm will unnecessarily shuffle
      // the states between replicas.
      int statePriority1 = _replica.getStatePriority();
      int statePriority2 = replica2._replica.getStatePriority();
      if (statePriority1 != statePriority2) {
        // Note we shall prioritize the replica with a higher state priority,
        // the smaller priority number means higher priority.
        return statePriority1 - statePriority2;
      }

      // 3. Sort according to the replica impact based on the weight.
      // So the greedy algorithm will place the replicas with larger impact first.
      int result = Float.compare(replica2._score, _score);
      if (result != 0) {
        return result;
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
      if (!_replicaHash.equals(replica2._replicaHash)) {
        return _replicaHash.compareTo(replica2._replicaHash);
      } else {
        // In case of hash collision, return order according to the name.
        return _replica.toString().compareTo(replica2.toString());
      }
    }
  }

  private AssignableReplica getNextAssignableReplica(
      Set<AssignableReplicaWithScore> allReplica,
      Map<String, Integer> overallClusterRemainingCapMap) {
    AssignableReplicaWithScore nextAssinableReplica = null;
    // Compare every replica with current candidate, update candidate if needed
    for (AssignableReplicaWithScore replica : allReplica) {
      replica.computeScore(overallClusterRemainingCapMap);
      if (nextAssinableReplica == null || replica.compareTo(nextAssinableReplica) < 0) {
        nextAssinableReplica = replica;
      }
    }
    allReplica.remove(nextAssinableReplica);
    return nextAssinableReplica.getAssignableReplica();
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
