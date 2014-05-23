package org.apache.helix.controller.strategy.knapsack;

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
