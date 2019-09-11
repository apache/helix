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
 * Evaluate the proposed assignment according to the other partitions assignment within the scope of
 * the same resource.
 * If the resource of the partition overall has a light load on the instance, the score is higher compared to
 * case when the resource is heavily loaded on the instance
 */
public class ResourcePartitionAntiAffinityConstraint extends SoftConstraint {

  @Override
  protected float getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    String resource = replica.getResourceName();
    int curPartitionCountForResource = node.getAssignedPartitionsByResource(resource).size();
    int doubleMaxPartitionCountForResource =
        2 * clusterContext.getEstimatedMaxPartitionByResource(resource);
    return Math.max(((float) doubleMaxPartitionCountForResource - curPartitionCountForResource)
        / doubleMaxPartitionCountForResource, 0);
  }
}
