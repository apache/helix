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

import java.util.Map;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;

class NodeCapacityConstraint extends HardConstraint {

  @Override
  boolean isAssignmentValid(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    Map<String, Integer> nodeCapacity = node.getRemainingCapacity();
    Map<String, Integer> replicaCapacity = replica.getCapacity();

    for (String key : replicaCapacity.keySet()) {
      if (nodeCapacity.containsKey(key)) {
        if (nodeCapacity.get(key) < replicaCapacity.get(key)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  String getDescription() {
    return "Node has insufficient capacity";
  }
}
