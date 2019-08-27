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

public class FaultZoneAwareConstraint extends HardConstraint {

    @Override
    boolean isAssignmentValid(AssignableNode node, AssignableReplica replica, ClusterContext clusterContext) {
        if (!node.hasFaultZone()) {
            return true;
        }
        return !clusterContext.getPartitionsForResourceAndFaultZone(replica.getResourceName(), node.getFaultZone())
                .contains(replica.getPartitionName());
    }

    @Override
    String getDescription() {
        return "A fault zone cannot contain more than 1 replicas of same partition";
    }
}
