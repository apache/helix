package org.apache.helix.controller.strategy.knapsack;

/**
 * Implementation of {@link KnapsackSearchPath}<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public class KnapsackSearchPathImpl implements KnapsackSearchPath {
  private KnapsackSearchNode _from;
  private KnapsackSearchNode _via;
  private KnapsackSearchNode _to;

  /**
   * Create a search path between nodes in a knapsack
   * @param from the source node
   * @param to the destination node
   */
  public KnapsackSearchPathImpl(final KnapsackSearchNode from, final KnapsackSearchNode to) {
    _from = from;
    _via = null;
    _to = to;
  }

  @Override
  public void init() {
    KnapsackSearchNode nodeFrom = moveUpToDepth(_from, _to.depth());
    KnapsackSearchNode nodeTo = moveUpToDepth(_to, _from.depth());
    if (nodeFrom.depth() != nodeTo.depth()) {
      throw new RuntimeException("to and from depths do not match!");
    }

    // Find common parent
    // TODO: check if basic equality is enough
    while (nodeFrom != nodeTo) {
      nodeFrom = nodeFrom.parent();
      nodeTo = nodeTo.parent();
    }
    _via = nodeFrom;
  }

  @Override
  public KnapsackSearchNode from() {
    return _from;
  }

  @Override
  public KnapsackSearchNode via() {
    return _via;
  }

  @Override
  public KnapsackSearchNode to() {
    return _to;
  }

  @Override
  public KnapsackSearchNode moveUpToDepth(KnapsackSearchNode node, int depth) {
    KnapsackSearchNode currentNode = node;
    while (currentNode.depth() > depth) {
      currentNode = currentNode.parent();
    }
    return currentNode;
  }

}
