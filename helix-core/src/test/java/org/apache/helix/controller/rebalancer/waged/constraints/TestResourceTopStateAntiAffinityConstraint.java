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

import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestResourceTopStateAntiAffinityConstraint {
  private AssignableReplica _testReplica;
  private AssignableNode _testNode;
  private ClusterContext _clusterContext;

  private final SoftConstraint _constraint = new ResourceTopStateAntiAffinityConstraint();

  @BeforeMethod
  public void init() {
    _testReplica = Mockito.mock(AssignableReplica.class);
    _testNode = Mockito.mock(AssignableNode.class);
    _clusterContext = Mockito.mock(ClusterContext.class);
  }

  @Test
  public void testGetAssignmentScoreWhenReplicaNotTopState() {
    when(_testReplica.isReplicaTopState()).thenReturn(false);
    float score = _constraint.getAssignmentScore(_testNode, _testReplica, _clusterContext);
    float normalizedScore =
        _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext);
    Assert.assertEquals(score, 0f);
    Assert.assertEquals(normalizedScore, 1f);
    verifyZeroInteractions(_clusterContext);
  }

  @Test
  public void testGetAssignmentScoreWhenReplicaIsTopStateHeavyLoad() {
    when(_testReplica.isReplicaTopState()).thenReturn(true);
    when(_testNode.getAssignedTopStatePartitionsCount()).thenReturn(20);
    float score = _constraint.getAssignmentScore(_testNode, _testReplica, _clusterContext);
    float normalizedScore =
        _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext);
    Assert.assertEquals(score, 21f);
    Assert.assertEquals(normalizedScore, 0.04758309f);
  }

  @Test
  public void testGetAssignmentScoreWhenReplicaIsTopStateLightLoad() {
    when(_testReplica.isReplicaTopState()).thenReturn(true);
    when(_testNode.getAssignedTopStatePartitionsCount()).thenReturn(0);
    float score = _constraint.getAssignmentScore(_testNode, _testReplica, _clusterContext);
    float normalizedScore =
        _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext);
    Assert.assertEquals(score, 1f);
    Assert.assertEquals(normalizedScore, 0.7615942f);
  }
}
