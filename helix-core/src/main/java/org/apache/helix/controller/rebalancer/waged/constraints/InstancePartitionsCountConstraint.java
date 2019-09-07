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
 * Evaluate by instance's current partition count versus estimated max partition count
 * Intuitively, Encourage the assignment if the instance's occupancy rate is below average;
 * Discourage the assignment if the instance's occupancy rate is above average
 */
class InstancePartitionsCountConstraint extends SoftConstraint {
  private static final float MIN_SCORE = 0;

  InstancePartitionsCountConstraint(int maxPossiblePartitionsNumber) {
    super(maxPossiblePartitionsNumber, MIN_SCORE);
  }

  @Override
  protected float getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    int estimatedMaxPartitionCount = clusterContext.getEstimatedMaxPartitionCount();
    int currentPartitionCount = node.getAssignedReplicaCount();

    if (currentPartitionCount == 0) {
      return getMaxScore();
    }

    return estimatedMaxPartitionCount / (float) currentPartitionCount;
  }

  @Override
  ScalerFunction getScalerFunction() {
    return score -> score / getMaxScore();
  }
}
