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
 * This constraint exists to make partitions belonging to the same resource be assigned as far from
 * each other as possible. This is because it is undesirable to have many partitions belonging to
 * the same resource be assigned to the same node to minimize the impact of node failure scenarios.
 * The score is higher the fewer the partitions are on the node belonging to the same resource.
 */
class ResourcePartitionAntiAffinityConstraint extends SoftConstraint {
  private static final float MAX_SCORE = 1f;
  private static final float MIN_SCORE = 0f;

  ResourcePartitionAntiAffinityConstraint() {
    super(MAX_SCORE, MIN_SCORE);
  }

  @Override
  protected float getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    String resource = replica.getResourceName();
    int curPartitionCountForResource = node.getAssignedPartitionsByResource(resource).size();
    int doubleMaxPartitionCountForResource =
        2 * clusterContext.getEstimatedMaxPartitionByResource(resource);
    // The score measures the twice the max allowed count versus current counts
    // The returned value is a measurement of remaining quota ratio, in the case of exceeding allowed counts, return 0
    return Math.max(((float) doubleMaxPartitionCountForResource - curPartitionCountForResource)
        / doubleMaxPartitionCountForResource, 0);
  }
}
