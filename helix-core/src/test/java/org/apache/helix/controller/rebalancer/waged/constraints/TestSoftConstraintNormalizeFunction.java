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

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSoftConstraintNormalizeFunction {
  @Test
  public void testDefaultNormalizeFunction() {
    int maxScore = 1000;
    int minScore = 0;
    SoftConstraint softConstraint = new SoftConstraint() {
      @Override
      protected float getAssignmentScore(AssignableNode node, AssignableReplica replica,
          ClusterContext clusterContext) {
        return 0;
      }
    };

    float currentScore = 1f;
    // verifies the function is decreasing when score gets larger and the normalized score range within (0, 1]
    for (int i = minScore; i <= maxScore; i++) {
      float normalized = softConstraint.getNormalizeFunction().scale(i);
      Assert.assertTrue(currentScore >= normalized,
          String.format("input: %s, output: %s", i, normalized));
      Assert.assertTrue(normalized <= 1 && normalized > 0,
          String.format("input: %s, output: %s", i, normalized));
      currentScore = normalized;
    }
  }
}
