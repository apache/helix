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
  // TODO: these factors will be tuned based on user's preference
  // This factor indicates the default score that is evaluated if only partition allocation matches
  // (states are different).
  // This factor indicates the contribution of the Baseline assignment matching to the final score.
  private float _baseLineMatchFactor = 0.25f;

  PartitionMovementConstraint(float baselineMatchFactor) {
    _baseLineMatchFactor = baselineMatchFactor;
  }

  PartitionMovementConstraint() {
  }

  @Override
  protected float getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    boolean isBestPossibleMatch =
        isMatch(node, replica, clusterContext.getBestPossibleAssignment());
    boolean isBaselineMatch = isMatch(node, replica, clusterContext.getBaselineAssignment());

    /*
     * The final follows the table look-up rule
     * factor = base line match factor
     * 1 - factor = best possible match factor
     * | True | False |
     * True | 1 | factor|
     * False| 1-factor | 0 |
     */
    return (isBaselineMatch ? 1 : 0) * _baseLineMatchFactor
        + (isBestPossibleMatch ? 1 : 0) * (1 - _baseLineMatchFactor);
  }

  @Override
  NormalizeFunction getNormalizeFunction() {
    return score -> score;
  }

  private boolean isMatch(AssignableNode node, AssignableReplica replica,
      Map<String, ResourceAssignment> assignmentMap) {
    if (assignmentMap == null || assignmentMap.isEmpty()) {
      return false;
    }
    String resourceName = replica.getResourceName();
    String partitionName = replica.getPartitionName();
    Map<String, String> instanceToStateMap =
        assignmentMap.get(resourceName).getReplicaMap(new Partition(partitionName));

    String instanceName = node.getInstanceName();
    return instanceToStateMap.containsKey(instanceName)
        && instanceToStateMap.get(instanceName).equals(replica.getReplicaState());
  }
}
