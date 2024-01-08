package org.apache.helix.controller.rebalancer.strategy;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;

/**
 * Assignment strategy interface that computes the assignment of partition->instance.
 */
public interface RebalanceStrategy<T extends BaseControllerDataProvider> {
  String DEFAULT_REBALANCE_STRATEGY = "DEFAULT";

  /**
   * Perform the necessary initialization for the rebalance strategy object.
   *
   * @param resourceName      The resource for assignment mapping computation.
   * @param partitions        The partition names of the resource.
   * @param states            The state -> required state count mapping for the resource, ordered by
   *                          priority.
   * @param maximumPerNode    The maximum number of partitions that can be assigned to a node.
   */
  void init(String resourceName, final List<String> partitions,
      final LinkedHashMap<String, Integer> states, int maximumPerNode);

  /**
   * Compute the preference lists and (optional partition-state mapping) for the given resource.
   *
   * @param liveNodes          The list of live nodes names.
   * @param currentMapping     The current mapping of partition->state->instance.
   * @param allNodes           The list of all node names. This could be filtered for evacuation
   *                           and swap operations.
   * @param clusterData        The cache of the cluster data snapshot from Zookeeper.
   * @return                   The computed IdealState with ListFields for preference lists (optional)
   *                           and MapFields for partition->state->instance mapping (required).
   */
  ZNRecord computePartitionAssignment(final List<String> allNodes, final List<String> liveNodes,
      final Map<String, Map<String, String>> currentMapping,
      T clusterData);
}
