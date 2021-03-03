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


/**
 * Evaluate the proposed assignment according to the potential partition movements cost based on
 * the previous best possible assignment.
 * The previous best possible assignment is the sole reference; if it's missing, it means the
 * replica belongs to a newly added resource, so baseline assignment should be used instead.
 */
public class PartitionMovementConstraint extends AbstractPartitionMovementConstraint {
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
      // if best possible is missing, it means the replica belongs to a newly added resource, so
      // baseline assignment should be used instead.
      return calculateAssignmentScore(nodeName, state, baselineAssignment);
    }
    return calculateAssignmentScore(nodeName, state, bestPossibleAssignment);
  }
}
