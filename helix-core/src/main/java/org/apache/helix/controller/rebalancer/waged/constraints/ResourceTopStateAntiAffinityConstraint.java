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

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;

/**
 * Evaluate the proposed assignment according to the top state replication count on the instance.
 * The higher number the number of top state partitions assigned to the instance, the lower the
 * score, vice versa.
 */
class ResourceTopStateAntiAffinityConstraint extends UsageSoftConstraint {
  @Override
  protected double getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    if (!replica.isReplicaTopState()) {
      // For non top state replica, this constraint is not applicable.
      // So return zero on any assignable node candidate.
      return 0;
    }
    int curTopPartitionCountForResource = node.getAssignedTopStatePartitionsCount();
    int estimatedMaxTopStateCount = clusterContext.getEstimatedMaxTopStateCount();
    return computeUtilizationScore(estimatedMaxTopStateCount, curTopPartitionCountForResource);
  }
}
