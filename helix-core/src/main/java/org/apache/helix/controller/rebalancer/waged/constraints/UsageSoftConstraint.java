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

    import org.apache.commons.math3.analysis.function.Sigmoid;

/**
 * The soft constraint that evaluates the assignment proposal based on usage.
 */
abstract class UsageSoftConstraint extends SoftConstraint {
  private static final double MAX_SCORE = 1f;
  private static final double MIN_SCORE = 0f;
  /**
   * Alpha is used to adjust how smooth the sigmoid function should be.
   * As tested, when we have the input number which surrounding 1, the default alpha value will
   * ensure a smooth curve (sigmoid(0.95) = 0.90, sigmoid(1.05) = 0.1).
   * This means if the usage is within +-5% difference compared with the estimated usage, the
   * evaluated score will be reasonably different so the rebalancer can decide accordingly.
   * Else, if the current usage is much less or more than the estimation, the score will be very
   * close to 1.0 (less than estimation), or very close to 0.1 (more than estimation).
   **/
  private static final int DEFAULT_ALPHA = 44;
  private static final Sigmoid SIGMOID = new Sigmoid();

  UsageSoftConstraint() {
    super(MAX_SCORE, MIN_SCORE);
  }

  /**
   * Compute utilization score based on the current usage and the estimated usage.
   * The score is evaluated using a sigmoid function.
   * When the usage is smaller than estimation, the constraint returns a value that is very close to
   * the max score.
   * When the usage is close or larger than the estimate, the constraint returns a score that is
   * very close to the min score. Note even in this case, more usage will still be assigned with a
   * smaller score.
   * @param estimatedUsage The estimated usage that is between [0.0, 1.0]
   * @param currentUsage The current usage that is between [0.0, 1.0]
   * @return The score between [0.0, 1.0] that evaluates the utilization.
   */
  protected double computeUtilizationScore(double estimatedUsage, double currentUsage) {
    if (estimatedUsage == 0) {
      return 0;
    }
    return SIGMOID.value(-(currentUsage / estimatedUsage - 1) * DEFAULT_ALPHA) * (MAX_SCORE
        - MIN_SCORE);
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
