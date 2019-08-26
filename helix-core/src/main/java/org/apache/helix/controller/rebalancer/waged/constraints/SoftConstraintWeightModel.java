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

import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * The class retrieves the offline model that defines the relative importance of soft constraints.
 */
class SoftConstraintWeightModel {
  private static Map<? extends SoftConstraint, Float> MODEL;

  static {
    // TODO either define the weights in property files or zookeeper node or static human input
    MODEL = ImmutableMap.<SoftConstraint, Float> builder()
        .put(LeastPartitionCountConstraint.INSTANCE, 1.0f).build();
  }

  /**
   * Get the sum of normalized scores, given calculated scores map of soft constraints
   * @param originScoresMap The origin scores by soft constraints
   * @return The sum of soft constraints scores
   */
  float getSumOfScores(Map<SoftConstraint, Float> originScoresMap) {
    float sum = 0;
    for (Map.Entry<SoftConstraint, Float> softConstraintScoreEntry : originScoresMap.entrySet()) {
      SoftConstraint softConstraint = softConstraintScoreEntry.getKey();
      float score = softConstraint.getScalerFunction().scale(softConstraintScoreEntry.getValue());
      float weight = MODEL.get(softConstraint);
      sum += score * weight;
    }

    return sum;
  }
}
