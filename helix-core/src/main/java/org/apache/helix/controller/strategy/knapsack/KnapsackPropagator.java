package org.apache.helix.controller.strategy.knapsack;

import java.util.ArrayList;

/**
 * Constraint enforcer for a single dimenstion on a knapsack solution search<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public interface KnapsackPropagator {
  /**
   * Initialize the propagator
   * @param profits profits for selecting each item
   * @param weights weights of each item for this dimension
   */
  void init(final ArrayList<Long> profits, final ArrayList<Long> weights);

  /**
   * Update the search
   * @param revert revert the assignment
   * @param assignment the assignment to use for the update
   * @return true if successful, false if failed
   */
  boolean update(boolean revert, final KnapsackAssignment assignment);

  /**
   * Compute the upper and lower bounds of potential profits
   */
  void computeProfitBounds();

  /**
   * Get the next item to use in the search
   * @return item id
   */
  int getNextItemId();

  /**
   * Get the current profit of the search
   * @return current profit
   */
  long currentProfit();

  /**
   * Get the lowest possible profit of the search
   * @return profit lower bound
   */
  long profitLowerBound();

  /**
   * Get the highest possible profit of the search
   * @return profit upper bound
   */
  long profitUpperBound();

  /**
   * Copy the current computed state to the final solution
   * @param hasOnePropagator true if there is only one propagator, i.e. 1 dimension
   * @param solution the solution vector
   */
  void copyCurrentStateToSolution(boolean hasOnePropagator, ArrayList<Boolean> solution);
}
