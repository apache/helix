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

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

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
  private final List<SoftConstraint> _softConstraints;
  private final SoftConstraintWeightModel _softConstraintsWeightModel;

  ConstraintBasedAlgorithm(List<HardConstraint> hardConstraints,
      List<SoftConstraint> softConstraints, SoftConstraintWeightModel softConstraintWeightModel) {
    _hardConstraints = hardConstraints;
    _softConstraints = softConstraints;
    _softConstraintsWeightModel = softConstraintWeightModel;
  }

  @Override
  public OptimalAssignment calculate(ClusterModel clusterModel) throws HelixRebalanceException {
    OptimalAssignment optimalAssignment = new OptimalAssignment();
    Map<String, Set<AssignableReplica>> replicasByResource = clusterModel.getAssignableReplicaMap();
    List<AssignableNode> nodes = new ArrayList<>(clusterModel.getAssignableNodes().values());

    // TODO: different orders of resource/replica could lead to different greedy assignments, will
    // revisit and improve the performance
    for (String resource : replicasByResource.keySet()) {
      for (AssignableReplica replica : replicasByResource.get(resource)) {
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
        maybeBestNode.ifPresent(node -> clusterModel.assign(replica.getResourceName(),
            replica.getPartitionName(), replica.getReplicaState(), node.getInstanceName()));
      }
    }

    return optimalAssignment.convertFrom(clusterModel);
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
        (candidateNode) -> _softConstraintsWeightModel.getSumOfScores(_softConstraints.stream()
            .collect(Collectors.toMap(Function.identity(), softConstraint -> softConstraint
                .getAssignmentNormalizedScore(candidateNode, replica, clusterContext))));

    return candidateNodes.stream().max(Comparator.comparing(calculatePoints));
  }

  private List<String> convertFailureReasons(List<HardConstraint> hardConstraints) {
    return hardConstraints.stream().map(HardConstraint::getDescription)
        .collect(Collectors.toList());
  }
}
