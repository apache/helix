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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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
    // Sort the replicas so the input is stable for the greedy algorithm.
    // For the other algorithm implementation, this sorting could be unnecessary.
    for (AssignableReplica replica : getOrderedAssignableReplica(clusterModel)) {
      Optional<AssignableNode> maybeBestNode =
          getNodeWithHighestPoints(replica, nodes, clusterModel.getContext(), optimalAssignment);
      // stop immediately if any replica cannot find best assignable node
      if (optimalAssignment.hasAnyFailure()) {
        String errorMessage = String.format(
            "Unable to find any available candidate node for partition %s; Fail reasons: %s",
            replica.getPartitionName(), optimalAssignment.getFailures());
        throw new HelixRebalanceException(errorMessage,
            HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      }
      maybeBestNode.ifPresent(node -> clusterModel
          .assign(replica.getResourceName(), replica.getPartitionName(), replica.getReplicaState(),
              node.getInstanceName()));
    }
    optimalAssignment.updateAssignments(clusterModel);
    return optimalAssignment;
  }

  private Optional<AssignableNode> getNodeWithHighestPoints(AssignableReplica replica,
      List<AssignableNode> assignableNodes, ClusterContext clusterContext,
      OptimalAssignment optimalAssignment) {
    Map<AssignableNode, List<HardConstraint>> hardConstraintFailures = new HashMap<>();
    List<AssignableNode> candidateNodes = assignableNodes.stream().filter(candidateNode -> {
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

    Function<AssignableNode, Float> calculatePoints =
        (candidateNode) -> getAssignmentNormalizedScore(candidateNode, replica, clusterContext);

    return candidateNodes.stream().max(Comparator.comparing(calculatePoints));
  }

  private float getAssignmentNormalizedScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    float sum = 0;
    for (Map.Entry<SoftConstraint, Float> softConstraintEntry : _softConstraints.entrySet()) {
      SoftConstraint softConstraint = softConstraintEntry.getKey();
      float weight = softConstraintEntry.getValue();
      if (weight != 0) {
        // Skip calculating zero weight constraints.
        sum += weight * softConstraint.getAssignmentNormalizedScore(node, replica, clusterContext);
      }
    }
    return sum;
  }

  private List<String> convertFailureReasons(List<HardConstraint> hardConstraints) {
    return hardConstraints.stream().map(HardConstraint::getDescription)
        .collect(Collectors.toList());
  }

  // TODO investigate better ways to sort replicas. One option is sorting based on the creation time.
  private List<AssignableReplica> getOrderedAssignableReplica(ClusterModel clusterModel) {
    Map<String, Set<AssignableReplica>> replicasByResource = clusterModel.getAssignableReplicaMap();
    List<AssignableReplica> orderedAssignableReplicas =
        replicasByResource.values().stream().flatMap(replicas -> replicas.stream())
            .collect(Collectors.toList());

    Map<String, ResourceAssignment> bestPossibleAssignment =
        clusterModel.getContext().getBestPossibleAssignment();
    Map<String, ResourceAssignment> baselineAssignment =
        clusterModel.getContext().getBaselineAssignment();

    // 1. Sort according if the assignment exists in the best possible and/or baseline assignment
    // 2. Sort according to the state priority. Note that prioritizing the top state is required.
    // Or the greedy algorithm will unnecessarily shuffle the states between replicas.
    // 3. Sort according to the resource/partition name.
    orderedAssignableReplicas.sort((replica1, replica2) -> {
      String resourceName1 = replica1.getResourceName();
      String resourceName2 = replica2.getResourceName();
      if (bestPossibleAssignment.containsKey(resourceName1) == bestPossibleAssignment
          .containsKey(resourceName2)) {
        if (baselineAssignment.containsKey(resourceName1) == baselineAssignment
            .containsKey(resourceName2)) {
          // If both assignment states have/not have the resource assignment the same,
          // compare for additional dimensions.
          int statePriority1 = replica1.getStatePriority();
          int statePriority2 = replica2.getStatePriority();
          if (statePriority1 == statePriority2) {
            // If state prioritizes are the same, compare the names.
            if (resourceName1.equals(resourceName2)) {
              return replica1.getPartitionName().compareTo(replica2.getPartitionName());
            } else {
              return resourceName1.compareTo(resourceName2);
            }
          } else {
            // Note we shall prioritize the replica with a higher state priority,
            // the smaller priority number means higher priority.
            return statePriority1 - statePriority2;
          }
        } else {
          // If the baseline assignment contains the assignment, prioritize the replica.
          return baselineAssignment.containsKey(resourceName1) ? -1 : 1;
        }
      } else {
        // If the best possible assignment contains the assignment, prioritize the replica.
        return bestPossibleAssignment.containsKey(resourceName1) ? -1 : 1;
      }
    });
    return orderedAssignableReplicas;
  }
}
