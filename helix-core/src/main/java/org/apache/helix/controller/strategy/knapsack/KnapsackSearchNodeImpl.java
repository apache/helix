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

/**
 * Implementation of {@link KnapsackSearchNode}<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public class KnapsackSearchNodeImpl implements KnapsackSearchNode {
  private static final int NO_SELECTION = -1;

  private int _depth;
  private KnapsackSearchNode _parent;
  private KnapsackAssignment _assignment;
  private long _currentProfit;
  private long _profitUpperBound;
  private int _nextItemId;

  /**
   * Initialize a search node
   * @param parent the node's parent
   * @param assignment the node's assignment
   */
  public KnapsackSearchNodeImpl(final KnapsackSearchNode parent, final KnapsackAssignment assignment) {
    _depth = (parent == null) ? 0 : parent.depth() + 1;
    _parent = parent;
    _assignment = assignment;
    _currentProfit = 0L;
    _profitUpperBound = Long.MAX_VALUE;
    _nextItemId = NO_SELECTION;
  }

  @Override
  public int depth() {
    return _depth;
  }

  @Override
  public KnapsackSearchNode parent() {
    return _parent;
  }

  @Override
  public KnapsackAssignment assignment() {
    return _assignment;
  }

  @Override
  public long currentProfit() {
    return _currentProfit;
  }

  @Override
  public void setCurrentProfit(long profit) {
    _currentProfit = profit;
  }

  @Override
  public long profitUpperBound() {
    return _profitUpperBound;
  }

  @Override
  public void setProfitUpperBound(long profit) {
    _profitUpperBound = profit;
  }

  @Override
  public int nextItemId() {
    return _nextItemId;
  }

  @Override
  public void setNextItemId(int id) {
    _nextItemId = id;
  }

}
