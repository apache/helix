package org.apache.helix.controller.strategy.knapsack;

/**
 * Basic structure of an item in a knapsack<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public class KnapsackItem {
  public final int id;
  public final long weight;
  public final long profit;

  /**
   * Initialize the item
   * @param id the item id
   * @param weight the cost to place the item in the knapsack for one dimension
   * @param profit the benefit of placing the item in the knapsack
   */
  public KnapsackItem(int id, long weight, long profit) {
    this.id = id;
    this.weight = weight;
    this.profit = profit;
  }

  /**
   * Get the profit to weight ratio
   * @param profitMax the maximum possible profit for this item
   * @return the item addition effciency
   */
  public double getEfficiency(long profitMax) {
    return (weight > 0) ? ((double) profit) / ((double) weight) : ((double) profitMax);
  }
}
