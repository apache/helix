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
  //TODO: these factors will be tuned based on user's preference
  // This factor indicates the default score that is evaluated if only partition allocation matches
  // (states are different).
  private static final double ALLOCATION_MATCH_FACTOR = 0.5;

  PartitionMovementConstraint() {
    super(MAX_SCORE, MIN_SCORE);
  }

  @Override
  protected double getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    // Prioritize the previous Best Possible assignment
    Map<String, String> bestPossibleAssignment =
        getStateMap(replica, clusterContext.getBestPossibleAssignment());
    if (!bestPossibleAssignment.isEmpty()) {
      return calculateAssignmentScale(node, replica, bestPossibleAssignment);
    }
    // else, compare the baseline only if the best possible assignment does not contain the replica
    Map<String, String> baselineAssignment =
        getStateMap(replica, clusterContext.getBaselineAssignment());
    if (!baselineAssignment.isEmpty()) {
      return calculateAssignmentScale(node, replica, baselineAssignment);
    }
    return 0;
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

  private double calculateAssignmentScale(AssignableNode node, AssignableReplica replica,
      Map<String, String> instanceToStateMap) {
    String instanceName = node.getInstanceName();
    if (!instanceToStateMap.containsKey(instanceName)) {
      return 0;
    } else {
      return (instanceToStateMap.get(instanceName).equals(replica.getReplicaState()) ? 1 :
          ALLOCATION_MATCH_FACTOR);
    }
  }

  @Override
  protected NormalizeFunction getNormalizeFunction() {
    // PartitionMovementConstraint already scale the score properly.
    return (score) -> score;
  }
}
