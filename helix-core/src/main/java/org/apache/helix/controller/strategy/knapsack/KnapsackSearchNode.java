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
 * Description of a knapsack element during the search process<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public interface KnapsackSearchNode {
  /**
   * Depth of the node in this search
   * @return node depth
   */
  int depth();

  /**
   * The parent node in this search
   * @return the node's immediate parent
   */
  KnapsackSearchNode parent();

  /**
   * The current node assignment
   * @return KnapsackAssignment instance
   */
  KnapsackAssignment assignment();

  /**
   * The current profit with this node and search
   * @return current profit
   */
  long currentProfit();

  /**
   * Set the current profit with this node and search
   * @param profit current profit
   */
  void setCurrentProfit(long profit);

  /**
   * The maximum possible profit with this node and search
   * @return profit upper bound
   */
  long profitUpperBound();

  /**
   * Set the maximum possible profit with this node and search
   * @param profit profit upper bound
   */
  void setProfitUpperBound(long profit);

  /**
   * The next item given this node and search
   * @return next item id
   */
  int nextItemId();

  /**
   * Set the next item given this node and search
   * @param id next item id
   */
  void setNextItemId(int id);
}
