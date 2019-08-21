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
  private float MIN_SCORE = Float.MIN_VALUE;
  private float MAX_SCORE = Float.MAX_VALUE;
  private static Map<SoftConstraint.Type, Float> MODEL;

  static {
    // TODO either define the weights in property files or zookeeper node or static human input
    MODEL = ImmutableMap.<SoftConstraint.Type, Float> builder()
        .put(SoftConstraint.Type.LEAST_MOVEMENTS, 1.0f)
        .put(SoftConstraint.Type.LEAST_PARTITION_COUNT, 1.0f)
        .put(SoftConstraint.Type.LEAST_USED_NODE, 1.0f).build();
  }

  interface ScoreScaler {
    /**
     * Method to scale the origin score to a normalized range
     * @param originScore The origin score of a range
     * @return The normalized value between 0 - 1
     */
    float scale(float originScore);
  }

  private ScoreScaler MIN_MAX_SCALER =
      originScore -> (originScore - MIN_SCORE) / (MAX_SCORE - MIN_SCORE);

  /**
   * Get the sum of normalized scores, given calculated scores map of soft constraints
   * @param originScoresMap The origin scores of soft constraints
   * @return The sum of double type
   */
  float getSumOfScores(Map<SoftConstraint, Float> originScoresMap) {
    float sum = 0;
    for (Map.Entry<SoftConstraint, Float> softConstraintScoreEntry : originScoresMap.entrySet()) {
      float score = MIN_MAX_SCALER.scale(softConstraintScoreEntry.getValue());
      float weight = MODEL.get(softConstraintScoreEntry.getKey().getType());
      sum += score * weight;
    }

    return sum;
  }
}
