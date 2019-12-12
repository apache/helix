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
 * Evaluate a partition allocation proposal and return YES or NO based on the cluster context.
 * Any proposal fails one or more hard constraints will be rejected.
 */
public interface HardConstraint {
  enum FailureReason {
    FAULT_ZONES_CONTAIN_SAME_PARTITION,
    NODES_DEACTIVATED,
    NODES_NO_TAG,
    NODES_EXCEED_MAX_PARTITION,
    NODES_INSUFFICIENT_RESOURCE,
    NODES_CONTAIN_SAME_PARTITION,
  }

  /**
   * @return True if the proposed assignment is valid.
   */
  boolean isAssignmentValid(AssignableNode node, AssignableReplica rep,
      ClusterContext clusterContext);

  /**
   * @return Detail of the reason that the proposed assignment was rejected.
   */
  FailureReason getFailureReason();
}
