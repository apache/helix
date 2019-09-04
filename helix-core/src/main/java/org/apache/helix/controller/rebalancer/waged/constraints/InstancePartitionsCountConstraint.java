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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;


/**
 * Evaluate by instance's current partition count and estimated max partition count
 */
class InstancePartitionsCountConstraint extends SoftConstraint {

  InstancePartitionsCountConstraint() {
  }

  InstancePartitionsCountConstraint(float maxScore, float minScore) {
    super(maxScore, minScore);
  }

  @Override
  float evaluateAssignment(AssignableNode node, AssignableReplica replica, ClusterContext clusterContext) {
    int estimatedMaxPartitionCount = clusterContext.getEstimatedMaxPartitionCount();
    int currentPartitionCount = node.getAssignedReplicaCount();
    // When the node is idle, return with the maxScore.
    if (currentPartitionCount == 0) {
      return getMaxScore();
    }
    //When the node usage reaches the estimated max partition, return minimal score
    if (currentPartitionCount >= estimatedMaxPartitionCount) {
      return getMinScore();
    }

    // Return the usage percentage by estimation
    return (estimatedMaxPartitionCount - currentPartitionCount) / (float) estimatedMaxPartitionCount;
  }
}