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
 * Evaluate the proposed assignment according to the instance's partition count.
 */
class LeastPartitionCountConstraint extends SoftConstraint {
  static LeastPartitionCountConstraint INSTANCE = new LeastPartitionCountConstraint();

  private LeastPartitionCountConstraint() {
  }

  /**
   * Returns a score depending on the number of assignments on this node. The score is scaled evenly
   * between the minScore and maxScore.
   * When the node is idle, return with the maxScore.
   * When the node usage reaches the estimated max partition, return with (minScore + maxScore ) /
   * 2.
   * When the node usage reaches 2 * estimated_max or more, return with the minScore.
   * If the estimated max partition count is not set, it defaults to Integer.MAX_VALUE in
   * clusterContext.
   */
  @Override
  float getAssignmentOriginScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    float doubleMaxPartitionCount = 2.0f * clusterContext.getEstimatedMaxPartitionCount();
    int curPartitionCount = node.getCurrentAssignmentCount();
    return Math.max((doubleMaxPartitionCount - curPartitionCount) / doubleMaxPartitionCount, 0);
  }
}
