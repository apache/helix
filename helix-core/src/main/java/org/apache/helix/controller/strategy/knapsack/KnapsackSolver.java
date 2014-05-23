package org.apache.helix.controller.strategy.knapsack;

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
