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

import com.google.common.collect.ImmutableSet;

public class TestResourcePartitionAntiAffinityConstraint {
  private static final String TEST_PARTITION = "TestPartition";
  private static final String TEST_RESOURCE = "TestResource";
  private final AssignableReplica _testReplica = Mockito.mock(AssignableReplica.class);
  private final AssignableNode _testNode = Mockito.mock(AssignableNode.class);
  private final ClusterContext _clusterContext = Mockito.mock(ClusterContext.class);
  private final SoftConstraint _constraint = new ResourcePartitionAntiAffinityConstraint();

  @Test
  public void testGetAssignmentScore() {
    when(_testReplica.getResourceName()).thenReturn(TEST_RESOURCE);
    when(_testNode.getAssignedPartitionsByResource(TEST_RESOURCE)).thenReturn(
        ImmutableSet.of(TEST_PARTITION + "1", TEST_PARTITION + "2", TEST_PARTITION + "3"));

    float score = _constraint.getAssignmentScore(_testNode, _testReplica, _clusterContext);
    float normalizedScore = _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext);
    Assert.assertEquals(score, 3f);
    Assert.assertEquals(normalizedScore, 0.32151276f);
  }

  @Test
  public void testGetAssignmentScoreMaxScore() {
    when(_testReplica.getResourceName()).thenReturn(TEST_RESOURCE);
    when(_testNode.getAssignedPartitionsByResource(TEST_RESOURCE)).thenReturn(Collections.emptySet());

    float score = _constraint.getAssignmentScore(_testNode, _testReplica, _clusterContext);
    float normalizedScore = _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext);
    Assert.assertEquals(score,0f);
    Assert.assertEquals(normalizedScore, 1f);
  }
}
