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
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * A generic knapsack solver that supports multiple dimensions<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public class KnapsackGenericSolverImpl extends AbstractBaseKnapsackSolver {
  private static final int MASTER_PROPAGATOR_ID = 0;
  private static final int NO_SELECTION = -1;

  private ArrayList<KnapsackPropagator> _propagators;
  private int _masterPropagatorId;
  private ArrayList<KnapsackSearchNode> _searchNodes;
  private KnapsackState _state;
  private long _bestSolutionProfit;
  private ArrayList<Boolean> _bestSolution;

  /**
   * Create the solver
   * @param solverName name of the solver
   */
  public KnapsackGenericSolverImpl(String solverName) {
    super(solverName);
    _propagators = new ArrayList<KnapsackPropagator>();
    _masterPropagatorId = MASTER_PROPAGATOR_ID;
    _searchNodes = new ArrayList<KnapsackSearchNode>();
    _state = new KnapsackStateImpl();
    _bestSolutionProfit = 0L;
    _bestSolution = new ArrayList<Boolean>();
  }

  @Override
  public void init(ArrayList<Long> profits, ArrayList<ArrayList<Long>> weights,
      ArrayList<Long> capacities) {
    clear();
    final int numberOfItems = profits.size();
    final int numberOfDimensions = weights.size();
    _state.init(numberOfItems);

    _bestSolution.clear();
    for (int i = 0; i < numberOfItems; i++) {
      _bestSolution.add(false);
    }

    for (int i = 0; i < numberOfDimensions; i++) {
      KnapsackPropagator propagator = new KnapsackCapacityPropagatorImpl(_state, capacities.get(i));
      propagator.init(profits, weights.get(i));
      _propagators.add(propagator);
    }
    _masterPropagatorId = MASTER_PROPAGATOR_ID;
  }

  public int getNumberOfItems() {
    return _state.getNumberOfItems();
  }

  @Override
  public long[] getLowerAndUpperBoundWhenItem(int itemId, boolean isItemIn, long lowerBound,
      long upperBound) {
    long[] result = {
        lowerBound, upperBound
    };
    KnapsackAssignment assignment = new KnapsackAssignment(itemId, isItemIn);
    final boolean fail = !incrementalUpdate(false, assignment);
    if (fail) {
      result[0] = 0L;
      result[1] = 0L;
    } else {
      result[0] =
          (hasOnePropagator()) ? _propagators.get(_masterPropagatorId).profitLowerBound() : 0L;
      result[1] = getAggregatedProfitUpperBound();
    }

    final boolean failRevert = !incrementalUpdate(true, assignment);
    if (failRevert) {
      result[0] = 0L;
      result[1] = 0L;
    }
    return result;
  }

  public void setMasterPropagatorId(int masterPropagatorId) {
    _masterPropagatorId = masterPropagatorId;
  }

  @Override
  public long solve() {
    _bestSolutionProfit = 0L;
    PriorityQueue<KnapsackSearchNode> searchQueue =
        new PriorityQueue<KnapsackSearchNode>(11,
            new KnapsackSearchNodeInDecreasingUpperBoundComparator());
    KnapsackAssignment assignment = new KnapsackAssignment(NO_SELECTION, true);
    KnapsackSearchNode rootNode = new KnapsackSearchNodeImpl(null, assignment);
    rootNode.setCurrentProfit(getCurrentProfit());
    rootNode.setProfitUpperBound(getAggregatedProfitUpperBound());
    rootNode.setNextItemId(getNextItemId());
    _searchNodes.add(rootNode);

    if (makeNewNode(rootNode, false)) {
      searchQueue.add(_searchNodes.get(_searchNodes.size() - 1));
    }
    if (makeNewNode(rootNode, true)) {
      searchQueue.add(_searchNodes.get(_searchNodes.size() - 1));
    }

    KnapsackSearchNode currentNode = rootNode;
    while (!searchQueue.isEmpty() && searchQueue.peek().profitUpperBound() > _bestSolutionProfit) {
      KnapsackSearchNode node = searchQueue.poll();

      // TODO: check if equality is enough
      if (node != currentNode) {
        KnapsackSearchPath path = new KnapsackSearchPathImpl(currentNode, node);
        path.init();
        final boolean noFail = updatePropagators(path);
        currentNode = node;
        if (!noFail) {
          throw new RuntimeException("solver failed to update propagators");
        }
      }

      if (makeNewNode(node, false)) {
        searchQueue.add(_searchNodes.get(_searchNodes.size() - 1));
      }
      if (makeNewNode(node, true)) {
        searchQueue.add(_searchNodes.get(_searchNodes.size() - 1));
      }
    }
    return _bestSolutionProfit;
  }

  @Override
  public boolean bestSolution(int itemId) {
    return _bestSolution.get(itemId);
  }

  private void clear() {
    _propagators.clear();
    _searchNodes.clear();
  }

  private boolean updatePropagators(final KnapsackSearchPath path) {
    boolean noFail = true;
    KnapsackSearchNode node = path.from();
    KnapsackSearchNode via = path.via();
    while (node != via) {
      noFail = incrementalUpdate(true, node.assignment()) && noFail;
      node = node.parent();
    }
    node = path.to();
    while (node != via) {
      noFail = incrementalUpdate(false, node.assignment()) && noFail;
      node = node.parent();
    }
    return noFail;
  }

  private boolean incrementalUpdate(boolean revert, final KnapsackAssignment assignment) {
    boolean noFail = _state.updateState(revert, assignment);
    for (KnapsackPropagator propagator : _propagators) {
      noFail = propagator.update(revert, assignment) && noFail;
    }
    return noFail;
  }

  private void updateBestSolution() {
    final long profitLowerBound =
        (hasOnePropagator()) ? _propagators.get(_masterPropagatorId).profitLowerBound()
            : _propagators.get(_masterPropagatorId).currentProfit();

    if (_bestSolutionProfit < profitLowerBound) {
      _bestSolutionProfit = profitLowerBound;
      _propagators.get(_masterPropagatorId).copyCurrentStateToSolution(hasOnePropagator(),
          _bestSolution);
    }
  }

  private boolean makeNewNode(final KnapsackSearchNode node, boolean isIn) {
    if (node.nextItemId() == NO_SELECTION) {
      return false;
    }
    KnapsackAssignment assignment = new KnapsackAssignment(node.nextItemId(), isIn);
    KnapsackSearchNode newNode = new KnapsackSearchNodeImpl(node, assignment);

    KnapsackSearchPath path = new KnapsackSearchPathImpl(node, newNode);
    path.init();
    final boolean noFail = updatePropagators(path);
    if (noFail) {
      newNode.setCurrentProfit(getCurrentProfit());
      newNode.setProfitUpperBound(getAggregatedProfitUpperBound());
      newNode.setNextItemId(getNextItemId());
      updateBestSolution();
    }

    KnapsackSearchPath revertPath = new KnapsackSearchPathImpl(newNode, node);
    revertPath.init();
    updatePropagators(revertPath);

    if (!noFail || newNode.profitUpperBound() < _bestSolutionProfit) {
      return false;
    }

    KnapsackSearchNode relevantNode = new KnapsackSearchNodeImpl(node, assignment);
    relevantNode.setCurrentProfit(newNode.currentProfit());
    relevantNode.setProfitUpperBound(newNode.profitUpperBound());
    relevantNode.setNextItemId(newNode.nextItemId());
    _searchNodes.add(relevantNode);

    return true;
  }

  private long getAggregatedProfitUpperBound() {
    long upperBound = Long.MAX_VALUE;
    for (KnapsackPropagator propagator : _propagators) {
      propagator.computeProfitBounds();
      final long propagatorUpperBound = propagator.profitUpperBound();
      upperBound = Math.min(upperBound, propagatorUpperBound);
    }
    return upperBound;
  }

  private boolean hasOnePropagator() {
    return _propagators.size() == 1;
  }

  private long getCurrentProfit() {
    return _propagators.get(_masterPropagatorId).currentProfit();
  }

  private int getNextItemId() {
    return _propagators.get(_masterPropagatorId).getNextItemId();
  }

  /**
   * A special comparator that orders knapsack search nodes in decreasing potential profit order
   */
  // TODO: check order
  private static class KnapsackSearchNodeInDecreasingUpperBoundComparator implements
      Comparator<KnapsackSearchNode> {
    @Override
    public int compare(KnapsackSearchNode node1, KnapsackSearchNode node2) {
      final long profitUpperBound1 = node1.profitUpperBound();
      final long profitUpperBound2 = node2.profitUpperBound();
      if (profitUpperBound1 == profitUpperBound2) {
        final long currentProfit1 = node1.currentProfit();
        final long currentProfit2 = node2.currentProfit();
        if (currentProfit1 > currentProfit2) {
          return -1;
        } else if (currentProfit1 < currentProfit2) {
          return 1;
        } else {
          return 0;
        }
      }
      if (profitUpperBound1 > profitUpperBound2) {
        return -1;
      } else if (profitUpperBound1 < profitUpperBound2) {
        return 1;
      } else {
        return 0;
      }
    }

  }
}
