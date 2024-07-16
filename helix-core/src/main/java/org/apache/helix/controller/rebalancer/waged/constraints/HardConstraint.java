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

import java.util.Set;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;

/**
 * Evaluate a partition allocation proposal and return YES or NO based on the cluster context.
 * Any proposal fails one or more hard constraints will be rejected.
 */
abstract class HardConstraint {

  protected Set<String> logEnabledClusters;

  /**
   * Check if the replica could be assigned to the node
   * @return True if the proposed assignment is valid; False otherwise
   */
  abstract boolean isAssignmentValid(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext);

  /**
   * Return class name by default as description if it's explanatory enough, child class could override
   * the method and add more detailed descriptions
   * @return The detailed description of hard constraint
   */
  String getDescription() {
    return getClass().getName();
  }

  /**
   * Check if the logging is enabled for the replica
   * @param clusterName The name of the cluster to be checked
   */
  public boolean isLoggingEnabled(String clusterName) {
    return logEnabledClusters != null && logEnabledClusters.contains(clusterName);
  }

  /**
   * Set the reference of the replicas that need to be logged.
   * @param logEnabledClusters The clusters that need to be logged
   */
  public void setLogEnabledClusters(Set<String> logEnabledClusters) {
    this.logEnabledClusters = logEnabledClusters;
  }

}
