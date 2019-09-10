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

import java.util.Map;
import java.util.function.Supplier;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;

/**
 * The constraint evaluates the max usage ratio of capacity on the instance
 */
class LeastUsedInstanceConstraint extends SoftConstraint {
  private static final String COMPUTE_KEY = "MaxCapacityUsage";
  private static final float MIN_SCORE = 0;
  private static final float MAX_SCORE = 1;

  LeastUsedInstanceConstraint() {
    super(MAX_SCORE, MIN_SCORE);
  }

  @Override
  protected float getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    Supplier<Float> supplier = () -> {
      Map<String, Integer> currentCapacity = node.getCurrentCapacity();
      Map<String, Integer> maxCapacity = node.getMaxCapacity();
      float maxUsageRatio = -1;
      for (String capacityKey : maxCapacity.keySet()) {
        // 1. Don't have to check if key exist in the other map
        // 2. Don't need to check if the max capacity is greater than current capacity
        // These rules will be respected in AssignableNode class initialization & updates
        maxUsageRatio = Math.max((maxCapacity.get(capacityKey) - currentCapacity.get(capacityKey))
            / (float) maxCapacity.get(capacityKey), maxUsageRatio);
      }
      return maxUsageRatio;
    };

    float maxCapacityUsage = node.getOrCompute(COMPUTE_KEY, supplier, Float.class);
    return (1.0f - maxCapacityUsage) / 2;
  }

  @Override
  NormalizeFunction getNormalizeFunction() {
    // round to 2 decimals float number
    return score -> super.getNormalizeFunction().scale((float) Math.round(score * 100) / 100);
  }
}
