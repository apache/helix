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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestNodeActivateConstraint {
  private static final String TEST_PARTITION = "TestPartition";
  private static final String TEST_RESOURCE = "TestResource";
  private final AssignableReplica _testReplica = Mockito.mock(AssignableReplica.class);
  private final AssignableNode _testNode = Mockito.mock(AssignableNode.class);
  private final ClusterContext _clusterContext = Mockito.mock(ClusterContext.class);
  private final HardConstraint _constraint = new NodeActivateConstraint();

  @Test
  public void testConstraintValid() {
    when(_testNode.isEnabled()).thenReturn(true);
    when(_testNode.isLive()).thenReturn(true);
    when(_testReplica.getResourceName()).thenReturn(TEST_RESOURCE);
    when(_testReplica.getPartitionName()).thenReturn(TEST_PARTITION);
    when(_testNode.getDisabledPartitionsMap())
        .thenReturn(ImmutableMap.of(TEST_PARTITION, Collections.emptyList()));
    Assert.assertTrue(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
    when(_testNode.getDisabledPartitionsMap())
        .thenReturn(ImmutableMap.of(TEST_PARTITION, ImmutableList.of("dummy")));
    Assert.assertTrue(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  @Test
  public void testConstraintInValidWhenNodeInactive() {
    when(_testNode.isEnabled()).thenReturn(false);
    when(_testNode.isLive()).thenReturn(true);
    Assert.assertFalse(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
    when(_testNode.isEnabled()).thenReturn(false);
    when(_testNode.isLive()).thenReturn(false);
    Assert.assertFalse(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
    when(_testNode.isEnabled()).thenReturn(true);
    when(_testNode.isLive()).thenReturn(false);
    Assert.assertFalse(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  @Test
  public void testConstraintInvalidWhenReplicaIsDisabled() {
    when(_testNode.isEnabled()).thenReturn(true);
    when(_testNode.isLive()).thenReturn(true);
    when(_testReplica.getResourceName()).thenReturn(TEST_RESOURCE);
    when(_testReplica.getPartitionName()).thenReturn(TEST_PARTITION);
    when(_testNode.getDisabledPartitionsMap())
        .thenReturn(ImmutableMap.of(TEST_PARTITION, ImmutableList.of(TEST_PARTITION)));
    Assert.assertTrue(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }
}
