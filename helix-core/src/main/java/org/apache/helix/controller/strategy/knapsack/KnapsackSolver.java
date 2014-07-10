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

import java.util.ArrayList;

/**
 * Interface for a factory of multidimensional 0-1 knapsack solvers that support reductions<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public interface KnapsackSolver {
  /**
   * Collection of supported algorithms
   */
  enum SolverType {
    /**
     * A solver that uses the branch-and-bound technique, supports multiple dimensions
     */
    KNAPSACK_MULTIDIMENSION_BRANCH_AND_BOUND_SOLVER
  }

  /**
   * Initialize the solver
   * @param profits profit for each element if selected
   * @param weights cost of each element in each dimension
   * @param capacities maximum total weight in each dimension
   */
  void init(final ArrayList<Long> profits, final ArrayList<ArrayList<Long>> weights,
      final ArrayList<Long> capacities);

  /**
   * Solve the knapsack problem
   * @return the approximated optimal weight
   */
  long solve();

  /**
   * Check if an element was selected in the optimal solution
   * @param itemId the index of the element to check
   * @return true if the item is present, false otherwise
   */
  boolean bestSolutionContains(int itemId);

  /**
   * Get the name of this solver
   * @return solver name
   */
  String getName();

  /**
   * Check if a reduction should be used to prune paths early on
   * @return true if reduction enabled, false otherwise
   */
  boolean useReduction();

  /**
   * Set whether a reduction should be used to prune paths early on
   * @param useReduction true to enable, false to disable
   */
  void setUseReduction(boolean useReduction);
}
