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
 * Construction of the path between search nodes in a knapsack<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public interface KnapsackSearchPath {
  /**
   * Initialize the path
   */
  void init();

  /**
   * Get the source node
   * @return starting KnapsackSearchNode
   */
  KnapsackSearchNode from();

  /**
   * Get the intermediate node
   * @return KnapsackSearchNode between source and destination
   */
  KnapsackSearchNode via();

  /**
   * Get the destination node
   * @return terminating KnapsackSearchNode
   */
  KnapsackSearchNode to();

  /**
   * Get an ancestor of a given search node
   * @param node the search node
   * @param depth the depth of the ancestor
   * @return the ancestor node
   */
  KnapsackSearchNode moveUpToDepth(final KnapsackSearchNode node, int depth);
}
