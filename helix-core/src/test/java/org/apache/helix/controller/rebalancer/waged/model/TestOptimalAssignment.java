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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.helix.HelixException;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestOptimalAssignment extends ClusterModelTestHelper {

  @BeforeClass
  public void initialize() {
    super.initialize();
  }

  @Test
  public void testUpdateAssignment() throws IOException {
    OptimalAssignment assignment = new OptimalAssignment();

    // update with empty cluster model
    assignment.updateAssignments(getDefaultClusterModel());
    Map<String, ResourceAssignment> optimalAssignmentMap =
        assignment.getOptimalResourceAssignment();
    Assert.assertEquals(optimalAssignmentMap, Collections.emptyMap());

    // update with valid assignment
    ClusterModel model = getDefaultClusterModel();
    model.assign(_resourceNames.get(0), _partitionNames.get(1), "SLAVE", _testInstanceId);
    model.assign(_resourceNames.get(0), _partitionNames.get(0), "MASTER", _testInstanceId);
    assignment.updateAssignments(model);
    optimalAssignmentMap = assignment.getOptimalResourceAssignment();
//    Assert.assertEquals(assignment.getTotalPartitionMovements(Collections.emptyMap()), 2);
//    Assert.assertEquals(assignment.getTotalPartitionMovements(optimalAssignmentMap), 0);
    Assert.assertEquals(optimalAssignmentMap.get(_resourceNames.get(0)).getMappedPartitions(),
        Arrays
            .asList(new Partition(_partitionNames.get(0)), new Partition(_partitionNames.get(1))));
    Assert.assertEquals(optimalAssignmentMap.get(_resourceNames.get(0))
            .getReplicaMap(new Partition(_partitionNames.get(1))),
        Collections.singletonMap(_testInstanceId, "SLAVE"));
    Assert.assertEquals(optimalAssignmentMap.get(_resourceNames.get(0))
            .getReplicaMap(new Partition(_partitionNames.get(0))),
        Collections.singletonMap(_testInstanceId, "MASTER"));
  }

  @Test(dependsOnMethods = "testUpdateAssignment")
  public void TestAssignmentFailure() throws IOException {
    OptimalAssignment assignment = new OptimalAssignment();
    ClusterModel model = getDefaultClusterModel();

    // record failure
    AssignableReplica targetFailureReplica =
        model.getAssignableReplicaMap().get(_resourceNames.get(0)).iterator().next();
    AssignableNode targetFailureNode = model.getAssignableNodesAsMap().get(_testInstanceId);
    assignment.recordAssignmentFailure(targetFailureReplica, Collections
        .singletonMap(targetFailureNode, Collections.singletonList("Assignment Failure!")));

    Assert.assertTrue(assignment.hasAnyFailure());

    assignment.updateAssignments(getDefaultClusterModel());
    try {
      assignment.getOptimalResourceAssignment();
      Assert.fail("Get optimal assignment shall fail because of the failure record.");
    } catch (HelixException ex) {
      Assert.assertTrue(ex.getMessage().startsWith(
          "Cannot get the optimal resource assignment since a calculation failure is recorded."));
    }
  }
}
