package org.apache.helix.controller.strategy.knapsack;

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
 * Common implementation of a knapsack solver<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public abstract class AbstractBaseKnapsackSolver implements BaseKnapsackSolver {
  private final String _solverName;

  /**
   * Initialize the solver
   * @param solverName the name of the solvers
   */
  public AbstractBaseKnapsackSolver(final String solverName) {
    _solverName = solverName;
  }

  @Override
  public long[] getLowerAndUpperBoundWhenItem(int itemId, boolean isItemIn, long lowerBound,
      long upperBound) {
    return new long[] {
        0L, Long.MAX_VALUE
    };
  }

  @Override
  public String getName() {
    return _solverName;
  }

}
