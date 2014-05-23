package org.apache.helix.controller.strategy.knapsack;

/**
 * Construction of the path between search nodes in a knapsack<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public interface KnapsackSearchPath {
  /**
   * Initialize the path
   */
  void init();

  /**
   * Get the source node
   * @return starting KnapsackSearchNode
   */
  KnapsackSearchNode from();

  /**
   * Get the intermediate node
   * @return KnapsackSearchNode between source and destination
   */
  KnapsackSearchNode via();

  /**
   * Get the destination node
   * @return terminating KnapsackSearchNode
   */
  KnapsackSearchNode to();

  /**
   * Get an ancestor of a given search node
   * @param node the search node
   * @param depth the depth of the ancestor
   * @return the ancestor node
   */
  KnapsackSearchNode moveUpToDepth(final KnapsackSearchNode node, int depth);
}
