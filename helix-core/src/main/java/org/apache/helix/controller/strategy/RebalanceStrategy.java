package org.apache.helix.controller.strategy;

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

import org.apache.helix.ZNRecord;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Assignment strategy interface that computes the assignment of partition->instance.
 */
public interface RebalanceStrategy {
  /**
   * Perform the necessary initialization for the rebalance strategy object.
   * @param resourceName
   * @param partitions
   * @param states
   * @param maximumPerNode
   */
  void init(String resourceName, final List<String> partitions,
      final LinkedHashMap<String, Integer> states, int maximumPerNode);

  /**
   * Compute the preference lists and (optional partition-state mapping) for the given resource.
   *
   * @param liveNodes
   * @param currentMapping
   * @param allNodes
   * @return
   */
  ZNRecord computePartitionAssignment(final List<String> liveNodes,
      final Map<String, Map<String, String>> currentMapping, final List<String> allNodes);
}
