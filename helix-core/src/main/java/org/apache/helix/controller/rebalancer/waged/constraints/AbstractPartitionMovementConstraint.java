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
 * Any change from the old assignment will increase the partition movements cost, so that the
 * evaluated score will become lower.
 */
abstract class AbstractPartitionMovementConstraint extends SoftConstraint {
  protected static final double MAX_SCORE = 1f;
  protected static final double MIN_SCORE = 0f;

  private static final double STATE_TRANSITION_COST_FACTOR = 0.5;

  AbstractPartitionMovementConstraint() {
    super(MAX_SCORE, MIN_SCORE);
  }

  /**
   * @return MAX_SCORE if the proposed assignment completely matches the previous assignment.
   *         StateTransitionCostFactor if the proposed assignment's allocation matches the
   *         previous assignment but state does not match.
   *         MIN_SCORE if the proposed assignment completely doesn't match the previous one.
   */
  @Override
  protected abstract double getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext);

  protected Map<String, String> getStateMap(AssignableReplica replica,
      Map<String, ResourceAssignment> assignment) {
    String resourceName = replica.getResourceName();
    String partitionName = replica.getPartitionName();
    if (assignment == null || !assignment.containsKey(resourceName)) {
      return Collections.emptyMap();
    }
    return assignment.get(resourceName).getReplicaMap(new Partition(partitionName));
  }

  protected double calculateAssignmentScore(String nodeName, String state,
      Map<String, String> instanceToStateMap) {
    if (instanceToStateMap.containsKey(nodeName)) {
      // The score when the proposed allocation partially matches the assignment plan but will
      // require a state transition.
      double scoreWithStateTransitionCost =
          MIN_SCORE + (MAX_SCORE - MIN_SCORE) * STATE_TRANSITION_COST_FACTOR;
      // if state matches, no state transition required for the proposed assignment; if state does
      // not match, then the proposed assignment requires state transition.
      return state.equals(instanceToStateMap.get(nodeName)) ? MAX_SCORE
          : scoreWithStateTransitionCost;
    }
    return MIN_SCORE;
  }

  @Override
  protected NormalizeFunction getNormalizeFunction() {
    return (score) -> score;
  }
}
