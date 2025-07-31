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

import java.util.List;

import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ReplicaActivateConstraint extends HardConstraint {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicaActivateConstraint.class);

  @Override
  boolean isAssignmentValid(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    List<String> disabledPartitions = node.getDisabledPartitionsMap().get(replica.getResourceName());
    boolean allResourcesDisabled = node.getDisabledPartitionsMap()
        .containsKey(InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY);

    if (allResourcesDisabled || (disabledPartitions != null && disabledPartitions.contains(replica.getPartitionName()))) {
      // Check if relaxed disabled partition constraint is enabled
      if (clusterContext.isRelaxedDisabledPartitionConstraintEnabled(replica.getResourceName())) {
        // In relaxed mode, allow assignment to disabled partitions (they will remain OFFLINE)
        if (enableLogging) {
          LOG.info("Relaxed mode: allowing assignment of replica {} to disabled partition", replica.getPartitionName());
        }
        return true;
      }
      
      // Default behavior: reject assignment to disabled partitions
      if (enableLogging) {
        LOG.info("Cannot assign the inactive replica: {}", replica.getPartitionName());
      }
      return false;
    }
    return true;
  }

  @Override
  String getDescription() {
    return "Cannot assign the inactive replica";
  }
}
