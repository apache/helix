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

/**
 * The soft constraint that evaluates the assignment proposal based on usage.
 */
abstract class UsageSoftConstraint extends SoftConstraint {
  private static final float MAX_SCORE = 1f;
  private static final float MIN_SCORE = 0f;

  UsageSoftConstraint() {
    super(MAX_SCORE, MIN_SCORE);
  }

  /**
   * Compute utilization score which is higher when the usage is low.
   * Considering the real usage could be larger than the estimated usage, use a segmented function
   * to convert the utilization number into range [0.0, 1.0].
   * In detail, when the current usage is smaller than the estimation, return a value between
   * [0.1, 1.0]. When the current usage is larger than the estimation, return a value between
   * [0.0, 0.1)
   * @param estimatedUsage
   * @param currentUsage
   * @return The score between [0.0, 1.0] that evaluates the utilization.
   */
  protected float computeUtilizationScore(float estimatedUsage, float currentUsage) {
    if (estimatedUsage <= 0) {
      return 0;
    }
    // Enlarge the estimated usage range so as to avoid oversensitive constraint evaluation
    estimatedUsage *= 2;
    if (currentUsage <= estimatedUsage) {
      return (estimatedUsage - currentUsage) / estimatedUsage * 0.9f + 0.1f;
    } else {
      return estimatedUsage / currentUsage * 0.1f;
    }
  }

  @Override
  protected NormalizeFunction getNormalizeFunction() {
    // By default, if the score is calculated by calling computeUtilizationScore, it has been scaled
    // properly.
    // Children classes that do not directly use computeUtilizationScore to compute the
    // score should override this method.
    return (score) -> score;
  }
}
