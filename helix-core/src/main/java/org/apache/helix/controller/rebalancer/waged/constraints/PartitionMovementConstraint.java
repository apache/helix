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

import java.util.Collections;
import java.util.Map;

import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;

/**
 * Evaluate the proposed assignment according to the potential partition movements cost.
 * The cost is evaluated based on the difference between the old assignment and the new assignment.
 * In detail, we consider the following two previous assignments as the base.
 * - Baseline assignment that is calculated regardless of the node state (online/offline).
 * - Previous Best Possible assignment.
 * Any change to these two assignments will increase the partition movements cost, so that the
 * evaluated score will become lower.
 */
class PartitionMovementConstraint extends SoftConstraint {
  private static final double MAX_SCORE = 1f;
  private static final double MIN_SCORE = 0f;
  // The scale factor to adjust score when the proposed allocation partially matches the assignment
  // plan but will require a state transition (with partition movement).
  private static final double STATE_TRANSITION_COST_FACTOR = 0.5;

  // Influence factor for baseline and best possible assignments. If baseline factor is larger, the
  // movement score will be greatly influenced by baseline, causing more movements and faster
  // convergence to baseline. Recommend not to modify unless extensive tuning is done.
  private double _baseline_influence_factor = 0.25;
  private double _best_possible_influence_factor = 1.25;

  private static final double CUSTOMIZED_BASELINE_INFLUENCE_FACTOR = 10000;
  private static final double CUSTOMIZED_BEST_POSSIBLE_INFLUENCE_FACTOR = 0.001;

  PartitionMovementConstraint(boolean useCustomizedMovementFactors) {
    super(MAX_SCORE, MIN_SCORE);
    if (useCustomizedMovementFactors) {
      try {
        _baseline_influence_factor = Double.parseDouble(System
            .getProperty(SystemPropertyKeys.PARTITION_MOVEMENT_BASELINE_INFLUENCE_FACTOR,
                String.valueOf(CUSTOMIZED_BASELINE_INFLUENCE_FACTOR)));
      } catch (NumberFormatException e) {
        _baseline_influence_factor = CUSTOMIZED_BASELINE_INFLUENCE_FACTOR;
      }
      try {
        _best_possible_influence_factor = Double.parseDouble(System
            .getProperty(SystemPropertyKeys.PARTITION_MOVEMENT_BEST_POSSIBLE_INFLUENCE_FACTOR,
                String.valueOf(CUSTOMIZED_BEST_POSSIBLE_INFLUENCE_FACTOR)));
      } catch (NumberFormatException e) {
        _best_possible_influence_factor = CUSTOMIZED_BEST_POSSIBLE_INFLUENCE_FACTOR;
      }
    }
  }

  /**
   * @return 1 if the proposed assignment completely matches the previous best possible assignment
   *         (or baseline assignment if the replica is newly added).
   *         STATE_TRANSITION_COST_FACTOR if the proposed assignment's allocation matches the
   *         previous Best Possible assignment (or baseline assignment if the replica is newly
   *         added) but state does not match.
   *         MOVEMENT_COST_FACTOR if the proposed assignment's allocation matches the baseline
   *         assignment only, but not matches the previous best possible assignment.
   *         0 if the proposed assignment is a pure random movement.
   */
  @Override
  protected double getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    Map<String, String> bestPossibleAssignment =
        getStateMap(replica, clusterContext.getBestPossibleAssignment());
    Map<String, String> baselineAssignment =
        getStateMap(replica, clusterContext.getBaselineAssignment());
    String nodeName = node.getInstanceName();
    String state = replica.getReplicaState();

    if (bestPossibleAssignment.isEmpty()) {
      // If bestPossibleAssignment of the replica is empty, indicating this is a new replica.
      // Then the baseline is the only reference.
      return calculateAssignmentScore(nodeName, state, baselineAssignment)
          * _best_possible_influence_factor;
    } else {
      return Math.max(calculateAssignmentScore(nodeName, state, baselineAssignment)
              * _baseline_influence_factor,
          calculateAssignmentScore(nodeName, state, bestPossibleAssignment)
              * _best_possible_influence_factor);
    }
  }

  private Map<String, String> getStateMap(AssignableReplica replica,
      Map<String, ResourceAssignment> assignment) {
    String resourceName = replica.getResourceName();
    String partitionName = replica.getPartitionName();
    if (assignment == null || !assignment.containsKey(resourceName)) {
      return Collections.emptyMap();
    }
    return assignment.get(resourceName).getReplicaMap(new Partition(partitionName));
  }

  private double calculateAssignmentScore(String nodeName, String state,
      Map<String, String> instanceToStateMap) {
    if (instanceToStateMap.containsKey(nodeName)) {
      return state.equals(instanceToStateMap.get(nodeName)) ?
          1 : // if state matches, no state transition required for the proposed assignment
          STATE_TRANSITION_COST_FACTOR; // if state does not match,
                                        // then the proposed assignment requires state transition.
    }
    return 0;
  }

  @Override
  protected NormalizeFunction getNormalizeFunction() {
    // PartitionMovementConstraint already scale the score properly.
    return (score) -> score;
  }
}
