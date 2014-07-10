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
 * The interface of any multidimensional knapsack solver<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public interface BaseKnapsackSolver {
  /**
   * Initialize the solver
   * @param profits profit of adding each item to the knapsack
   * @param weights cost of adding each item in each dimension
   * @param capacities maximum weight per dimension
   */
  void init(final ArrayList<Long> profits, final ArrayList<ArrayList<Long>> weights,
      final ArrayList<Long> capacities);

  /**
   * Compute an upper and lower bound on the knapsack given the assignment state of the knapsack
   * @param itemId the item id
   * @param isItemIn true if the item is in the knapsack, false otherwise
   * @param lowerBound the current lower bound
   * @param upperBound the current upper bound
   * @return the new lower and upper bounds
   */
  long[] getLowerAndUpperBoundWhenItem(int itemId, boolean isItemIn, long lowerBound,
      long upperBound);

  /**
   * Solve the knapsack problem
   * @return the (approximate) optimal profit
   */
  long solve();

  /**
   * Check if an item is in the final solution
   * @param itemId the item id
   * @return true if the item is present, false otherwise
   */
  boolean bestSolution(int itemId);

  /**
   * Get the solver name
   * @return solver name
   */
  String getName();
}
