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
  private static final double MAX_SCORE = 1f;
  private static final double MIN_SCORE = 0f;
  // The scale factor to adjust score when the proposed allocation partially matches the assignment
  // plan but will require a state transition (with partition movement).
  // TODO: these factors will be tuned based on user's preference
  private static final double STATE_TRANSITION_COST_FACTOR = 0.3;

  AbstractPartitionMovementConstraint() {
    super(MAX_SCORE, MIN_SCORE);
  }

  /**
   * @return 1 if the proposed assignment completely matches the previous assignment.
   *         STATE_TRANSITION_COST_FACTOR if the proposed assignment's allocation matches the
   *         previous assignment but state does not match.
   */
  @Override
  protected double getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    return calculateAssignmentScore(node.getInstanceName(), replica.getReplicaState(),
        getStateMap(replica, clusterContext));
  }

  protected abstract Map<String, String> getStateMap(AssignableReplica replica,
      ClusterContext clusterContext);

  protected Map<String, String> getStateMapFromAssignment(AssignableReplica replica,
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
