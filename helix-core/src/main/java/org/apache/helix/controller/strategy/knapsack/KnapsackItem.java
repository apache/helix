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
