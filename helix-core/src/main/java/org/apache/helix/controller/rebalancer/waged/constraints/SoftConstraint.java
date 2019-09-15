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

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;

/**
 * The "soft" constraint evaluates the optimality of an assignment by giving it a score of a scale
 * of [minScore, maxScore]
 * The higher the score, the better the assignment; Intuitively, the assignment is encouraged.
 * The lower score the score, the worse the assignment; Intuitively, the assignment is penalized.
 */
abstract class SoftConstraint {
  private float _maxScore = 1000f;
  private float _minScore = -1000f;

  interface NormalizeFunction {
    /**
     * Scale the origin score to a normalized range (0, 1).
     * The purpose is to compare scores between different soft constraints.
     * @param originScore The origin score
     * @return The normalized value between (0, 1)
     */
    float scale(float originScore);
  }

  /**
   * Default constructor, uses default min/max scores
   */
  SoftConstraint() {
  }

  /**
   * Child class customize the min/max score on its own
   * @param maxScore The max score
   * @param minScore The min score
   */
  SoftConstraint(float maxScore, float minScore) {
    _maxScore = maxScore;
    _minScore = minScore;
  }

  float getMaxScore() {
    return _maxScore;
  }

  float getMinScore() {
    return _minScore;
  }

  /**
   * Evaluate and give a score for an potential assignment partition -> instance
   * Child class only needs to care about how the score is implemented
   * @return The score of the assignment in float value
   */
  protected abstract float getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext);

  /**
   * Evaluate and give a score for an potential assignment partition -> instance
   * It's the only exposed method to the caller
   * @return The score is normalized to be within MinScore and MaxScore
   */
  float getAssignmentNormalizedScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    return getNormalizeFunction().scale(getAssignmentScore(node, replica, clusterContext));
  }

  /**
   * The default scaler function that squashes any score within (min_score, max_score) to (0, 1);
   * Child class could override the method and customize the method on its own
   * @return The MinMaxScaler instance by default
   */
  NormalizeFunction getNormalizeFunction() {
    return (score) -> (score - getMinScore()) / (getMaxScore() - getMinScore());
  }
}
