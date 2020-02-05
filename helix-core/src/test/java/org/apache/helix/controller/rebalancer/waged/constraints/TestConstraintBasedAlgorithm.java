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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelTestHelper;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestConstraintBasedAlgorithm {
  private ConstraintBasedAlgorithm _algorithm;

  @BeforeMethod
  public void beforeMethod() {
    HardConstraint mockHardConstraint = mock(HardConstraint.class);
    SoftConstraint mockSoftConstraint = mock(SoftConstraint.class);
    when(mockHardConstraint.isAssignmentValid(any(), any(), any())).thenReturn(false);
    when(mockSoftConstraint.getAssignmentNormalizedScore(any(), any(), any())).thenReturn(1.0);

    _algorithm = new ConstraintBasedAlgorithm(ImmutableList.of(mockHardConstraint),
        ImmutableMap.of(mockSoftConstraint, 1f));
  }

  @Test(expectedExceptions = HelixRebalanceException.class)
  public void testCalculateNoValidAssignment() throws IOException, HelixRebalanceException {
    ClusterModel clusterModel = new ClusterModelTestHelper().getDefaultClusterModel();
    _algorithm.calculate(clusterModel);
  }

  @Test
  public void testCalculateWithValidAssignment() throws IOException, HelixRebalanceException {
    HardConstraint mockHardConstraint = mock(HardConstraint.class);
    SoftConstraint mockSoftConstraint = mock(SoftConstraint.class);
    when(mockHardConstraint.isAssignmentValid(any(), any(), any())).thenReturn(true);
    when(mockSoftConstraint.getAssignmentNormalizedScore(any(), any(), any())).thenReturn(1.0);
    _algorithm = new ConstraintBasedAlgorithm(ImmutableList.of(mockHardConstraint),
        ImmutableMap.of(mockSoftConstraint, 1f));
    ClusterModel clusterModel = new ClusterModelTestHelper().getDefaultClusterModel();
    OptimalAssignment optimalAssignment = _algorithm.calculate(clusterModel);

    Assert.assertFalse(optimalAssignment.hasAnyFailure());
  }
}
