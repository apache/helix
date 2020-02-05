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

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;

public class TestSamePartitionOnInstanceConstraint {
  private static final String TEST_RESOURCE = "TestResource";
  private static final String TEST_PARTITIOIN = TEST_RESOURCE + "0";
  private final AssignableReplica _testReplica = Mockito.mock(AssignableReplica.class);
  private final AssignableNode _testNode = Mockito.mock(AssignableNode.class);
  private final ClusterContext _clusterContext = Mockito.mock(ClusterContext.class);
  private final HardConstraint _constraint = new SamePartitionOnInstanceConstraint();

  @Test
  public void testConstraintValid() {
    when(_testNode.getAssignedPartitionsByResource(TEST_RESOURCE))
        .thenReturn(ImmutableSet.of("dummy"));
    when(_testReplica.getResourceName()).thenReturn(TEST_RESOURCE);
    when(_testReplica.getPartitionName()).thenReturn(TEST_PARTITIOIN);

    Assert.assertTrue(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  @Test
  public void testConstraintInValid() {
    when(_testNode.getAssignedPartitionsByResource(TEST_RESOURCE))
        .thenReturn(ImmutableSet.of(TEST_PARTITIOIN));
    when(_testReplica.getResourceName()).thenReturn(TEST_RESOURCE);
    when(_testReplica.getPartitionName()).thenReturn(TEST_PARTITIOIN);
    Assert.assertFalse(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }
}
