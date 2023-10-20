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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class NodeMaxPartitionLimitConstraint extends HardConstraint {

  private static final Logger LOG = LoggerFactory.getLogger(NodeMaxPartitionLimitConstraint.class);

  @Override
  boolean isAssignmentValid(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    boolean exceedMaxPartitionLimit =
        node.getMaxPartition() < 0 || node.getAssignedReplicaCount() < node.getMaxPartition();

    if (!exceedMaxPartitionLimit) {
      LOG.debug("Cannot exceed the max number of partitions ({}) limitation on node. Assigned replica count: {}",
          node.getMaxPartition(), node.getAssignedReplicaCount());
      return false;
    }

    int resourceMaxPartitionsPerInstance = replica.getResourceMaxPartitionsPerInstance();
    int assignedPartitionsByResourceSize = node.getAssignedPartitionsByResource(replica.getResourceName()).size();
    boolean exceedResourceMaxPartitionLimit = resourceMaxPartitionsPerInstance < 0
        || assignedPartitionsByResourceSize < resourceMaxPartitionsPerInstance;

    if (!exceedResourceMaxPartitionLimit) {
      LOG.debug("Cannot exceed the max number of partitions per resource ({}) limitation on node. Assigned replica count: {}",
          resourceMaxPartitionsPerInstance, assignedPartitionsByResourceSize);
      return false;
    }
    return true;
  }

  @Override
  String getDescription() {
    return "Cannot exceed the maximum number of partitions limitation on node";
  }
}
