package org.apache.helix.controller.strategy.knapsack;

/**
 * The assignment of a knapsack item to a knapsack<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public class KnapsackAssignment {
  public int itemId;
  public boolean isIn;

  /**
   * Create the assignment
   * @param itemId the item id
   * @param isIn true if the item is in the knapsack, false otherwise
   */
  public KnapsackAssignment(int itemId, boolean isIn) {
    this.itemId = itemId;
    this.isIn = isIn;
  }
}
