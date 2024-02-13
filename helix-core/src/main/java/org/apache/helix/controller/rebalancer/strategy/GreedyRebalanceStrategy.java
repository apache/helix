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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.common.CapacityNode;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GreedyRebalanceStrategy implements RebalanceStrategy<ResourceControllerDataProvider> {
  private static Logger logger = LoggerFactory.getLogger(GreedyRebalanceStrategy.class);
  private String _resourceName;
  private List<String> _partitions;
  private LinkedHashMap<String, Integer> _states;

  public GreedyRebalanceStrategy() {
  }

  @Override
  public void init(String resourceName, final List<String> partitions,
      final LinkedHashMap<String, Integer> states, int maximumPerNode) {
    _resourceName = resourceName;
    _partitions = partitions;
    _states = states;
  }

  @Override
  public ZNRecord computePartitionAssignment(final List<String> allNodes, final List<String> liveNodes,
      final Map<String, Map<String, String>> currentMapping, ResourceControllerDataProvider clusterData) {
    int numReplicas = countStateReplicas();
    ZNRecord znRecord = new ZNRecord(_resourceName);
    if (liveNodes.size() == 0) {
      return znRecord;
    }

    if (clusterData.getSimpleCapacitySet() == null) {
      logger.warn("No capacity set for resource: " + _resourceName);
      return znRecord;
    }

    // Sort the assignable nodes by id
    List<CapacityNode> assignableNodes = new ArrayList<>(clusterData.getSimpleCapacitySet());
    Collections.sort(assignableNodes, Comparator.comparing(CapacityNode::getId));

    // Assign partitions to node by order.
    for (int i = 0, index = 0; i < _partitions.size(); i++) {
      int startIndex = index;
      List<String> preferenceList = new ArrayList<>();
      for (int j = 0; j < numReplicas; j++) {
        while (index - startIndex < assignableNodes.size()) {
          CapacityNode node = assignableNodes.get(index++ % assignableNodes.size());
          if (node.canAdd(_resourceName, _partitions.get(i))) {
            preferenceList.add(node.getId());
            break;
          }
        }

        if (index - startIndex >= assignableNodes.size()) {
          // If the all nodes have been tried out, then no node can be assigned.
          logger.warn("No enough assignable nodes for resource: " + _resourceName);
        }
      }
      znRecord.setListField(_partitions.get(i), preferenceList);
    }

    return znRecord;
  }

  private int countStateReplicas() {
    int total = 0;
    for (Integer count : _states.values()) {
      total += count;
    }
    return total;
  }
}
