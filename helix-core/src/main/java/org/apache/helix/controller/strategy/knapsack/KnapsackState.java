package org.apache.helix.controller.strategy.knapsack;

/**
 * The current state of the knapsack<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public interface KnapsackState {
  /**
   * Initialize the knapsack with the number of items
   * @param numberOfItems the number of items
   */
  void init(int numberOfItems);

  /**
   * Update this state with an assignment
   * @param revert true to revert to the previous state, false otherwise
   * @param assignment the assignment that was made
   * @return true on success, false on failure
   */
  boolean updateState(boolean revert, final KnapsackAssignment assignment);

  /**
   * Get the current number of items in the knapsack
   * @return number of items
   */
  int getNumberOfItems();

  /**
   * Check if an item is currently bound to the knapsack
   * @param id the item id
   * @return true if bound, false otherwise
   */
  boolean isBound(int id);

  /**
   * Check if an item is currently in the knapsack
   * @param id the item id
   * @return true if inside, false otherwise
   */
  boolean isIn(int id);
}
