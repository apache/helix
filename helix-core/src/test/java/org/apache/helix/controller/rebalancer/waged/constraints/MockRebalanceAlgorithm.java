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

import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.mockito.Mockito.when;

/**
 * A mock up rebalance algorithm for unit test.
 * Note that the mock algorithm won't propagate the existing assignment to the output as a real
 * algorithm will do. This is for the convenience of testing.
 */
public class MockRebalanceAlgorithm implements RebalanceAlgorithm {
  Map<String, ResourceAssignment> _resultHistory = Collections.emptyMap();

  @Override
  public OptimalAssignment calculate(ClusterModel clusterModel) {
    // If no predefined rebalance result setup, do card dealing.
    Map<String, ResourceAssignment> result = new HashMap<>();
    Iterator<AssignableNode> nodeIterator =
        clusterModel.getAssignableNodes().values().stream().sorted().iterator();
    for (String resource : clusterModel.getAssignableReplicaMap().keySet()) {
      Iterator<AssignableReplica> replicaIterator =
          clusterModel.getAssignableReplicaMap().get(resource).stream().sorted().iterator();
      while (replicaIterator.hasNext()) {
        AssignableReplica replica = replicaIterator.next();
        if (!nodeIterator.hasNext()) {
          nodeIterator = clusterModel.getAssignableNodes().values().stream().sorted().iterator();
        }
        AssignableNode node = nodeIterator.next();

        // Put the assignment
        ResourceAssignment assignment = result.computeIfAbsent(replica.getResourceName(),
            resourceName -> new ResourceAssignment(resourceName));
        Partition partition = new Partition(replica.getPartitionName());
        if (assignment.getReplicaMap(partition).isEmpty()) {
          assignment.addReplicaMap(partition, new HashMap<>());
        }
        assignment.getReplicaMap(partition).put(node.getInstanceName(), replica.getReplicaState());
      }
    }

    _resultHistory = result;

    // Mock the return value for supporting test.
    OptimalAssignment optimalAssignment = Mockito.mock(OptimalAssignment.class);
    when(optimalAssignment.getOptimalResourceAssignment()).thenReturn(result);
    return optimalAssignment;
  }

  public Map<String, ResourceAssignment> getRebalanceResult() {
    return _resultHistory;
  }
}
