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
   * Alpha is used to adjust the curve of sigmoid function.
   * Intuitively, this is for tolerating the inaccuracy of the estimation.
   * Ideally, if we have the prefect estimation, we can use a segmented function here, which
   * scores the assignment with 1.0 if projected usage is below the estimation, and scores 0.0
   * if the projected usage exceeds the estimation. However, in reality, it is hard to get a
   * prefect estimation. With the curve of sigmoid, the algorithm reacts differently and
   * reasonally even the usage is a little bit more or less than the estimation for a certain
   * extend.
   * As tested, when we have the input number which surrounds 1, the default alpha value will
   * ensure a curve that has sigmoid(0.95) = 0.90, sigmoid(1.05) = 0.1. Meaning the constraint
   * can handle the estimation inaccuracy of +-5%.
   * To adjust the curve:
   * 1. Smaller alpha will increase the curve's scope. So the function will be handler a wilder
   * range of inaccuracy. However, the downside is more random movements since the evenness
   * score would be more changable and nondefinitive.
   * 2. Larger alpha will decrease the curve's scope. In that case, we might want to change to
   * use segmented function so as to speed up the algorthm.
   **/
  private static final int DEFAULT_ALPHA = 44;
  private static final Sigmoid SIGMOID = new Sigmoid();

  UsageSoftConstraint() {
    super(MAX_SCORE, MIN_SCORE);
  }

  /**
   * Compute the utilization score based on the estimated and current usage numbers.
   * The score = currentUsage / estimatedUsage.
   * In short, a smaller score means better assignment proposal.
   *
   * @param estimatedUsage The estimated usage that is between [0.0, 1.0]
   * @param currentUsage   The current usage that is between [0.0, 1.0]
   * @return The score between [0.0, 1.0] that evaluates the utilization.
   */
  protected double computeUtilizationScore(double estimatedUsage, double currentUsage) {
    if (estimatedUsage == 0) {
      return 0;
    }
    return currentUsage / estimatedUsage;
  }

  /**
   * Compute evaluation score based on the utilization data.
   * The normalized score is evaluated using a sigmoid function.
   * When the usage is smaller than 1.0, the constraint returns a value that is very close to the
   * max score.
   * When the usage is close or larger than 1.0, the constraint returns a score that is very close
   * to the min score. Note even in this case, more usage will still be assigned with a
   * smaller score.
   */
  @Override
  protected NormalizeFunction getNormalizeFunction() {
    return (score) -> SIGMOID.value(-(score - 1) * DEFAULT_ALPHA) * (MAX_SCORE - MIN_SCORE);
  }
}
