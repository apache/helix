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

/**
 * Implementation of {@link KnapsackSolver}<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public class KnapsackSolverImpl implements KnapsackSolver {
  private final BaseKnapsackSolver _solver;
  private final ArrayList<Boolean> _knownValue;
  private final ArrayList<Boolean> _bestSolution;
  private final ArrayList<Integer> _mappingReducedItemId;
  private boolean _isProblemSolved;
  private long _additionalProfit;
  private boolean _useReduction;

  /**
   * Initialize a generic knapsack solver
   * @param solverName the name of the solver
   */
  public KnapsackSolverImpl(String solverName) {
    _solver = new KnapsackGenericSolverImpl(solverName);
    _knownValue = new ArrayList<Boolean>();
    _bestSolution = new ArrayList<Boolean>();
    _mappingReducedItemId = new ArrayList<Integer>();
    _isProblemSolved = false;
    _additionalProfit = 0L;
    _useReduction = true;
  }

  /**
   * Initialize a specified knapsack solver
   * @param solverType the type of solver
   * @param solverName the name of the solver
   */
  public KnapsackSolverImpl(SolverType solverType, String solverName) {
    _knownValue = new ArrayList<Boolean>();
    _bestSolution = new ArrayList<Boolean>();
    _mappingReducedItemId = new ArrayList<Integer>();
    _isProblemSolved = false;
    _additionalProfit = 0L;
    _useReduction = true;
    BaseKnapsackSolver solver = null;
    switch (solverType) {
    case KNAPSACK_MULTIDIMENSION_BRANCH_AND_BOUND_SOLVER:
      solver = new KnapsackGenericSolverImpl(solverName);
      break;
    default:
      throw new RuntimeException("Solver " + solverType + " not supported");
    }
    _solver = solver;
  }

  @Override
  public void init(ArrayList<Long> profits, ArrayList<ArrayList<Long>> weights,
      ArrayList<Long> capacities) {
    _additionalProfit = 0L;
    _isProblemSolved = false;
    _solver.init(profits, weights, capacities);
    if (_useReduction) {
      final int numItems = profits.size();
      final int numReducedItems = reduceProblem(numItems);

      if (numReducedItems > 0) {
        computeAdditionalProfit(profits);
      }

      if (numReducedItems > 0 && numReducedItems < numItems) {
        initReducedProblem(profits, weights, capacities);
      }
    }
  }

  @Override
  public long solve() {
    return _additionalProfit + ((_isProblemSolved) ? 0 : _solver.solve());
  }

  @Override
  public boolean bestSolutionContains(int itemId) {
    final int mappedItemId = (_useReduction) ? _mappingReducedItemId.get(itemId) : itemId;
    return (_useReduction && _knownValue.get(itemId)) ? _bestSolution.get(itemId) : _solver
        .bestSolution(mappedItemId);
  }

  @Override
  public String getName() {
    return _solver.getName();
  }

  @Override
  public boolean useReduction() {
    return _useReduction;
  }

  @Override
  public void setUseReduction(boolean useReduction) {
    _useReduction = useReduction;
  }

  private int reduceProblem(int numItems) {
    _knownValue.clear();
    _bestSolution.clear();
    _mappingReducedItemId.clear();
    ArrayList<Long> j0UpperBounds = new ArrayList<Long>();
    ArrayList<Long> j1UpperBounds = new ArrayList<Long>();
    for (int i = 0; i < numItems; i++) {
      _knownValue.add(false);
      _bestSolution.add(false);
      _mappingReducedItemId.add(i);
      j0UpperBounds.add(Long.MAX_VALUE);
      j1UpperBounds.add(Long.MAX_VALUE);
    }
    _additionalProfit = 0L;
    long bestLowerBound = 0L;
    for (int itemId = 0; itemId < numItems; itemId++) {
      long upperBound = 0L;
      long lowerBound = Long.MAX_VALUE;
      long[] bounds = _solver.getLowerAndUpperBoundWhenItem(itemId, false, upperBound, lowerBound);
      lowerBound = bounds[0];
      upperBound = bounds[1];
      j1UpperBounds.set(itemId, upperBound);
      bestLowerBound = Math.max(bestLowerBound, lowerBound);
      bounds = _solver.getLowerAndUpperBoundWhenItem(itemId, true, upperBound, lowerBound);
      lowerBound = bounds[0];
      upperBound = bounds[1];
      j0UpperBounds.set(itemId, upperBound);
      bestLowerBound = Math.max(bestLowerBound, lowerBound);
    }

    int numReducedItems = 0;
    for (int itemId = 0; itemId < numItems; itemId++) {
      if (bestLowerBound > j0UpperBounds.get(itemId)) {
        _knownValue.set(itemId, true);
        _bestSolution.set(itemId, false);
        numReducedItems++;
      } else if (bestLowerBound > j1UpperBounds.get(itemId)) {
        _knownValue.set(itemId, true);
        _bestSolution.set(itemId, true);
        numReducedItems++;
      }
    }
    _isProblemSolved = numReducedItems == numItems;
    return numReducedItems;
  }

  private void computeAdditionalProfit(final ArrayList<Long> profits) {
    final int numItems = profits.size();
    _additionalProfit = 0L;
    for (int itemId = 0; itemId < numItems; itemId++) {
      if (_knownValue.get(itemId) && _bestSolution.get(itemId)) {
        _additionalProfit += profits.get(itemId);
      }
    }
  }

  private void initReducedProblem(final ArrayList<Long> profits,
      final ArrayList<ArrayList<Long>> weights, final ArrayList<Long> capacities) {
    final int numItems = profits.size();
    final int numDimensions = capacities.size();

    ArrayList<Long> reducedProfits = new ArrayList<Long>();
    for (int itemId = 0; itemId < numItems; itemId++) {
      if (!_knownValue.get(itemId)) {
        _mappingReducedItemId.set(itemId, reducedProfits.size());
        reducedProfits.add(profits.get(itemId));
      }
    }

    ArrayList<ArrayList<Long>> reducedWeights = new ArrayList<ArrayList<Long>>();
    ArrayList<Long> reducedCapacities = new ArrayList<Long>(capacities);
    for (int dim = 0; dim < numDimensions; dim++) {
      final ArrayList<Long> oneDimensionWeights = weights.get(dim);
      ArrayList<Long> oneDimensionReducedWeights = new ArrayList<Long>();
      for (int itemId = 0; itemId < numItems; itemId++) {
        if (_knownValue.get(itemId)) {
          if (_bestSolution.get(itemId)) {
            reducedCapacities
                .set(dim, reducedCapacities.get(dim) - oneDimensionWeights.get(itemId));
          }
        } else {
          oneDimensionReducedWeights.add(oneDimensionWeights.get(itemId));
        }
      }
      reducedWeights.add(oneDimensionReducedWeights);
    }
    _solver.init(reducedProfits, reducedWeights, reducedCapacities);
  }
}
