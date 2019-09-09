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

import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;

public class TestFaultZoneAwareConstraint {
  private static final String TEST_PARTITION = "testPartition";
  private static final String TEST_ZONE = "testZone";
  private static final String TEST_RESOURCE = "testResource";
  private final AssignableReplica _testReplica = Mockito.mock(AssignableReplica.class);
  private final AssignableNode _testNode = Mockito.mock(AssignableNode.class);
  private final ClusterContext _clusterContext = Mockito.mock(ClusterContext.class);

  private final HardConstraint _faultZoneAwareConstraint = new FaultZoneAwareConstraint();

  @BeforeMethod
  public void init() {
    when(_testReplica.getResourceName()).thenReturn(TEST_RESOURCE);
    when(_testReplica.getPartitionName()).thenReturn(TEST_PARTITION);
    when(_testNode.getFaultZone()).thenReturn(TEST_ZONE);
  }

  @Test
  public void inValidWhenFaultZoneAlreadyAssigned() {
    when(_testNode.hasFaultZone()).thenReturn(true);
    when(_clusterContext.getPartitionsForResourceAndFaultZone(TEST_RESOURCE, TEST_ZONE)).thenReturn(
            ImmutableSet.of(TEST_PARTITION));

    Assert.assertFalse(
        _faultZoneAwareConstraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  @Test
  public void validWhenEmptyAssignment() {
    when(_testNode.hasFaultZone()).thenReturn(true);
    when(_clusterContext.getPartitionsForResourceAndFaultZone(TEST_RESOURCE, TEST_ZONE)).thenReturn(Collections.emptySet());

    Assert.assertTrue(
        _faultZoneAwareConstraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  @Test
  public void validWhenNoFaultZone() {
    when(_testNode.hasFaultZone()).thenReturn(false);

    Assert.assertTrue(
        _faultZoneAwareConstraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }
}
