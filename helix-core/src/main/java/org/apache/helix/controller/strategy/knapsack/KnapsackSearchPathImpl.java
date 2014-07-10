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
 * Implementation of {@link KnapsackSearchPath}<br/>
 * <br/>
 * Based on the C++ knapsack solver in Google's or-tools package.
 */
public class KnapsackSearchPathImpl implements KnapsackSearchPath {
  private KnapsackSearchNode _from;
  private KnapsackSearchNode _via;
  private KnapsackSearchNode _to;

  /**
   * Create a search path between nodes in a knapsack
   * @param from the source node
   * @param to the destination node
   */
  public KnapsackSearchPathImpl(final KnapsackSearchNode from, final KnapsackSearchNode to) {
    _from = from;
    _via = null;
    _to = to;
  }

  @Override
  public void init() {
    KnapsackSearchNode nodeFrom = moveUpToDepth(_from, _to.depth());
    KnapsackSearchNode nodeTo = moveUpToDepth(_to, _from.depth());
    if (nodeFrom.depth() != nodeTo.depth()) {
      throw new RuntimeException("to and from depths do not match!");
    }

    // Find common parent
    // TODO: check if basic equality is enough
    while (nodeFrom != nodeTo) {
      nodeFrom = nodeFrom.parent();
      nodeTo = nodeTo.parent();
    }
    _via = nodeFrom;
  }

  @Override
  public KnapsackSearchNode from() {
    return _from;
  }

  @Override
  public KnapsackSearchNode via() {
    return _via;
  }

  @Override
  public KnapsackSearchNode to() {
    return _to;
  }

  @Override
  public KnapsackSearchNode moveUpToDepth(KnapsackSearchNode node, int depth) {
    KnapsackSearchNode currentNode = node;
    while (currentNode.depth() > depth) {
      currentNode = currentNode.parent();
    }
    return currentNode;
  }

}
