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
  // TODO: these factors will be tuned based on user's preference
  private static final double STATE_TRANSITION_COST_FACTOR = 0.5;
  private static final double MOVEMENT_COST_FACTOR = 0.25;

  PartitionMovementConstraint() {
    super(MAX_SCORE, MIN_SCORE);
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
      return calculateAssignmentScore(nodeName, state, baselineAssignment);
    } else {
      // Else, for minimizing partition movements or state transitions, prioritize the proposed
      // assignment that matches the previous Best Possible assignment.
      double score = calculateAssignmentScore(nodeName, state, bestPossibleAssignment);
      // If no Best Possible assignment matches, check the baseline assignment.
      if (score == 0 && baselineAssignment.containsKey(nodeName)) {
        // Although not desired, the proposed assignment that matches the baseline is still better
        // than a random movement. So try to evaluate the score with the MOVEMENT_COST_FACTOR
        // punishment.
        score = MOVEMENT_COST_FACTOR;
      }
      return score;
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
          // if state matches, no state transition required for the proposed assignment
          1 :
          // if state does not match, then the proposed assignment requires state transition
          STATE_TRANSITION_COST_FACTOR;
    }
    return 0;
  }

  @Override
  protected NormalizeFunction getNormalizeFunction() {
    // PartitionMovementConstraint already scale the score properly.
    return (score) -> score;
  }
}
