package org.apache.helix.controller.rebalancer.waged.model;

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

import org.apache.helix.HelixException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TestClusterModel extends AbstractTestClusterModel {
  @BeforeClass
  public void initialize() {
    super.initialize();
  }

  Set<AssignableNode> generateNodes(ResourceControllerDataProvider testCache) {
    Set<AssignableNode> nodeSet = new HashSet<>();
    testCache.getInstanceConfigMap().values().stream().forEach(config -> nodeSet
        .add(new AssignableNode(testCache, config.getInstanceName(), Collections.emptyList())));
    return nodeSet;
  }

  @Test
  public void testNormalUsage() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    Set<AssignableReplica> assignableReplicas = generateReplicas(testCache);
    Set<AssignableNode> assignableNodes = generateNodes(testCache);

    // Test 1 - initialization
    ClusterContext context = new ClusterContext(assignableReplicas, 2);
    ClusterModel clusterModel =
        new ClusterModel(context, assignableReplicas, assignableNodes, Collections.emptyMap(),
            Collections.emptyMap());

    Assert.assertTrue(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .allMatch(v -> v.values().isEmpty()));
    Assert.assertFalse(clusterModel.getAssignableNodes().values().stream()
        .anyMatch(n -> n.getCurrentAssignmentCount() != 0));

    // The initialization of the context, node and replication has been tested separately. So for
    // cluster model, focus on testing the assignment and release.

    // Assign
    AssignableReplica replica = assignableReplicas.iterator().next();
    AssignableNode node = assignableNodes.iterator().next();
    clusterModel
        .assign(replica.getResourceName(), replica.getPartitionName(), replica.getReplicaState(),
            node.getInstanceName());

    Assert.assertTrue(
        clusterModel.getContext().getAssignmentForFaultZoneMap().get(node.getFaultZone())
            .get(replica.getResourceName()).contains(replica.getPartitionName()));
    Assert.assertTrue(node.getCurrentAssignmentsMap().get(replica.getResourceName())
        .contains(replica.getPartitionName()));

    // Assign a non-exist replication
    try {
      clusterModel.assign("NOT-EXIST", replica.getPartitionName(), replica.getReplicaState(),
          node.getInstanceName());
      Assert.fail("Assigning a non existing resource partition shall fail.");
    } catch (HelixException ex) {
      // expected
    }

    // Assign a non-exist replication
    try {
      clusterModel
          .assign(replica.getResourceName(), replica.getPartitionName(), replica.getReplicaState(),
              "NON-EXIST");
      Assert.fail("Assigning a resource partition to a non existing instance shall fail.");
    } catch (HelixException ex) {
      // expected
    }

    // Release
    clusterModel
        .release(replica.getResourceName(), replica.getPartitionName(), replica.getReplicaState(),
            node.getInstanceName());

    Assert.assertTrue(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .allMatch(v -> v.values().stream().allMatch(s -> s.isEmpty())));
    Assert.assertFalse(clusterModel.getAssignableNodes().values().stream()
        .anyMatch(n -> n.getCurrentAssignmentCount() != 0));
  }
}
