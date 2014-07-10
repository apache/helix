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
import java.util.Collections;
import java.util.Comparator;

/**
 * A knapsack propagator that constrains assignments based on knapsack capacity for a given
 * dimension<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public class KnapsackCapacityPropagatorImpl extends AbstractKnapsackPropagator {
  private static final long ALL_BITS_64 = 0xFFFFFFFFFFFFFFFFL;
  private static final int NO_SELECTION = -1;

  private long _capacity;
  private long _consumedCapacity;
  private int _breakItemId;
  private ArrayList<KnapsackItem> _sortedItems;
  private long _profitMax;

  /**
   * Initialize the propagator
   * @param state the current knapsack state
   * @param capacity the knapsack capacity for this dimension
   */
  public KnapsackCapacityPropagatorImpl(KnapsackState state, long capacity) {
    super(state);
    _capacity = capacity;
    _consumedCapacity = 0L;
    _breakItemId = NO_SELECTION;
    _sortedItems = new ArrayList<KnapsackItem>();
    _profitMax = 0L;
  }

  @Override
  public void computeProfitBounds() {
    setProfitLowerBound(currentProfit());
    _breakItemId = NO_SELECTION;

    long remainingCapacity = _capacity - _consumedCapacity;
    int breakSortedItemId = NO_SELECTION;
    final int numberOfSortedItems = _sortedItems.size();
    for (int sortedId = 0; sortedId < numberOfSortedItems; sortedId++) {
      final KnapsackItem item = _sortedItems.get(sortedId);
      if (!state().isBound(item.id)) {
        _breakItemId = item.id;

        if (remainingCapacity >= item.weight) {
          remainingCapacity -= item.weight;
          setProfitLowerBound(profitLowerBound() + item.profit);
        } else {
          breakSortedItemId = sortedId;
          break;
        }
      }
    }
    setProfitUpperBound(profitLowerBound());
    if (breakSortedItemId != NO_SELECTION) {
      final long additionalProfit = getAdditionalProfit(remainingCapacity, breakSortedItemId);
      setProfitUpperBound(profitUpperBound() + additionalProfit);
    }
  }

  @Override
  public int getNextItemId() {
    return _breakItemId;
  }

  @Override
  protected void initPropagator() {
    _consumedCapacity = 0L;
    _breakItemId = NO_SELECTION;
    _sortedItems = new ArrayList<KnapsackItem>(items());
    _profitMax = 0L;
    for (KnapsackItem item : _sortedItems) {
      _profitMax = Math.max(_profitMax, item.profit);
    }
    _profitMax++;
    Collections.sort(_sortedItems, new KnapsackItemDecreasingEfficiencyComparator(_profitMax));
  }

  @Override
  protected boolean updatePropagator(boolean revert, KnapsackAssignment assignment) {
    if (assignment.isIn) {
      if (revert) {
        _consumedCapacity -= items().get(assignment.itemId).weight;
      } else {
        _consumedCapacity += items().get(assignment.itemId).weight;
        if (_consumedCapacity > _capacity) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  protected void copyCurrentStateToSolutionPropagator(ArrayList<Boolean> solution) {
    if (solution == null) {
      throw new RuntimeException("solution cannot be null!");
    }
    long remainingCapacity = _capacity - _consumedCapacity;
    for (KnapsackItem item : _sortedItems) {
      if (!state().isBound(item.id)) {
        if (remainingCapacity >= item.weight) {
          remainingCapacity -= item.weight;
          solution.set(item.id, true);
        } else {
          return;
        }
      }
    }
  }

  private long getAdditionalProfit(long remainingCapacity, int breakItemId) {
    final int afterBreakItemId = breakItemId + 1;
    long additionalProfitWhenNoBreakItem = 0L;
    if (afterBreakItemId < _sortedItems.size()) {
      final long nextWeight = _sortedItems.get(afterBreakItemId).weight;
      final long nextProfit = _sortedItems.get(afterBreakItemId).profit;
      additionalProfitWhenNoBreakItem =
          upperBoundOfRatio(remainingCapacity, nextProfit, nextWeight);
    }

    final int beforeBreakItemId = breakItemId - 1;
    long additionalProfitWhenBreakItem = 0L;
    if (beforeBreakItemId >= 0) {
      final long previousWeight = _sortedItems.get(beforeBreakItemId).weight;
      if (previousWeight != 0) {
        final long previousProfit = _sortedItems.get(beforeBreakItemId).profit;
        final long overusedCapacity = _sortedItems.get(breakItemId).weight - remainingCapacity;
        final long ratio = upperBoundOfRatio(overusedCapacity, previousProfit, previousWeight);

        additionalProfitWhenBreakItem = _sortedItems.get(breakItemId).profit - ratio;
      }
    }

    final long additionalProfit =
        Math.max(additionalProfitWhenNoBreakItem, additionalProfitWhenBreakItem);
    return additionalProfit;
  }

  private int mostSignificantBitsPosition64(long n) {
    int b = 0;
    if (0 != (n & (ALL_BITS_64 << (1 << 5)))) {
      b |= (1 << 5);
      n >>= (1 << 5);
    }
    if (0 != (n & (ALL_BITS_64 << (1 << 4)))) {
      b |= (1 << 4);
      n >>= (1 << 4);
    }
    if (0 != (n & (ALL_BITS_64 << (1 << 3)))) {
      b |= (1 << 3);
      n >>= (1 << 3);
    }
    if (0 != (n & (ALL_BITS_64 << (1 << 2)))) {
      b |= (1 << 2);
      n >>= (1 << 2);
    }
    if (0 != (n & (ALL_BITS_64 << (1 << 1)))) {
      b |= (1 << 1);
      n >>= (1 << 1);
    }
    if (0 != (n & (ALL_BITS_64 << (1 << 0)))) {
      b |= (1 << 0);
    }
    return b;
  }

  private boolean willProductOverflow(long value1, long value2) {
    final int mostSignificantBitsPosition1 = mostSignificantBitsPosition64(value1);
    final int mostSignificantBitsPosition2 = mostSignificantBitsPosition64(value2);
    final int overflow = 61;
    return mostSignificantBitsPosition1 + mostSignificantBitsPosition2 > overflow;
  }

  private long upperBoundOfRatio(long numerator1, long numerator2, long denominator) {
    if (!willProductOverflow(numerator1, numerator2)) {
      final long numerator = numerator1 * numerator2;
      final long result = numerator / denominator;
      return result;
    } else {
      final double ratio = (((double) numerator1) * ((double) numerator2)) / ((double) denominator);
      final long result = ((long) Math.floor(ratio + 0.5));
      return result;
    }
  }

  /**
   * A special comparator that orders knapsack items by decreasing efficiency (profit to weight
   * ratio)
   */
  private static class KnapsackItemDecreasingEfficiencyComparator implements
      Comparator<KnapsackItem> {
    private final long _profitMax;

    public KnapsackItemDecreasingEfficiencyComparator(long profitMax) {
      _profitMax = profitMax;
    }

    @Override
    public int compare(KnapsackItem item1, KnapsackItem item2) {
      double eff1 = item1.getEfficiency(_profitMax);
      double eff2 = item2.getEfficiency(_profitMax);
      if (eff1 < eff2) {
        return 1;
      } else if (eff1 > eff2) {
        return -1;
      } else {
        return 0;
      }
    }

  }
}
