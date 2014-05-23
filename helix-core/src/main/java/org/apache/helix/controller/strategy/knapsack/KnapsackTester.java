package org.apache.helix.controller.strategy.knapsack;

import java.util.ArrayList;

public class KnapsackTester {
  public static void main(String[] args) {
    // Construct an example
    long[] PROFITS = {
        96, 76, 56, 11, 86, 10, 66, 86, 83, 12, 9, 81
    };
    long[][] WEIGHTS = {
        {
            19, 1, 10, 1, 1, 14, 152, 11, 1, 1, 1, 1
        }, {
            0, 4, 53, 0, 0, 80, 0, 4, 5, 0, 0, 0
        }, {
            4, 660, 3, 0, 30, 0, 3, 0, 4, 90, 0, 0
        }, {
            7, 0, 18, 6, 770, 330, 7, 0, 0, 6, 0, 0
        }, {
            0, 20, 0, 4, 52, 3, 0, 0, 0, 5, 4, 0
        }, {
            0, 0, 40, 70, 4, 63, 0, 0, 60, 0, 4, 0
        }, {
            0, 32, 0, 0, 0, 5, 0, 3, 0, 660, 0, 9
        }
    };
    long[] CAPACITIES = {
        18209, 7692, 1333, 924, 26638, 61188, 13360
    };
    ArrayList<Long> profits = new ArrayList<Long>();
    for (long profit : PROFITS) {
      profits.add(profit);
    }
    ArrayList<ArrayList<Long>> weights = new ArrayList<ArrayList<Long>>();
    for (long[] innerWeights : WEIGHTS) {
      ArrayList<Long> singleWeights = new ArrayList<Long>();
      for (long weight : innerWeights) {
        singleWeights.add(weight);
      }
      weights.add(singleWeights);
    }
    ArrayList<Long> capacities = new ArrayList<Long>();
    for (long capacity : CAPACITIES) {
      capacities.add(capacity);
    }

    // Solve
    KnapsackSolver solver = new KnapsackSolverImpl("mySolver");
    solver.init(profits, weights, capacities);
    long result = solver.solve();
    System.err.println(result);
    for (int i = 0; i < profits.size(); i++) {
      System.err.println(solver.bestSolutionContains(i));
    }
  }

}
