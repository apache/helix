package org.apache.helix.controller.strategy.knapsack;

import java.util.ArrayList;

/**
 * Implementation of {@link KnapsackState}<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public class KnapsackStateImpl implements KnapsackState {
  private ArrayList<Boolean> _isBound;
  private ArrayList<Boolean> _isIn;

  /**
   * Initialize the knapsack state
   */
  public KnapsackStateImpl() {
    _isBound = new ArrayList<Boolean>();
    _isIn = new ArrayList<Boolean>();
  }

  @Override
  public void init(int numberOfItems) {
    _isBound.clear();
    _isIn.clear();
    for (int i = 0; i < numberOfItems; i++) {
      _isBound.add(false);
      _isIn.add(false);
    }
  }

  @Override
  public boolean updateState(boolean revert, KnapsackAssignment assignment) {
    if (revert) {
      _isBound.set(assignment.itemId, false);
    } else {
      if (_isBound.get(assignment.itemId) && _isIn.get(assignment.itemId) != assignment.isIn) {
        return false;
      }
      _isBound.set(assignment.itemId, true);
      _isIn.set(assignment.itemId, assignment.isIn);
    }
    return true;
  }

  @Override
  public int getNumberOfItems() {
    return _isBound.size();
  }

  @Override
  public boolean isBound(int id) {
    return _isBound.get(id);
  }

  @Override
  public boolean isIn(int id) {
    return _isIn.get(id);
  }

}
