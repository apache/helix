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

import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestNodeMaxPartitionLimitConstraint {
  private static final String TEST_RESOURCE = "TestResource";
  private final AssignableReplica _testReplica = Mockito.mock(AssignableReplica.class);
  private final AssignableNode _testNode = Mockito.mock(AssignableNode.class);
  private final ClusterContext _clusterContext = Mockito.mock(ClusterContext.class);
  private final HardConstraint _constraint = new NodeMaxPartitionLimitConstraint();

  @Test
  public void testConstraintValid() {
    when(_testNode.getCurrentAssignmentCount()).thenReturn(0);
    when(_testNode.getMaxPartition()).thenReturn(10);
    when(_testNode.getCurrentAssignmentsByResource(TEST_RESOURCE))
        .thenReturn(Collections.emptySet());
    when(_testReplica.getResourceMaxPartitionsPerInstance()).thenReturn(5);
    Assert.assertTrue(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  @Test
  public void testConstraintInvalid() {
    when(_testNode.getCurrentAssignmentCount()).thenReturn(10);
    when(_testNode.getMaxPartition()).thenReturn(5);
    Assert.assertFalse(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }
}
