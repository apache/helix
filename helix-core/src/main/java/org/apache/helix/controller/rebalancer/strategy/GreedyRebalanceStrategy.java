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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.controller.common.CapacityNode;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
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
  public ZNRecord computePartitionAssignment(final List<String> allNodes,
      final List<String> liveNodes, final Map<String, Map<String, String>> currentMapping,
      ResourceControllerDataProvider clusterData) {
    int numReplicas = countStateReplicas();
    ZNRecord znRecord = new ZNRecord(_resourceName);
    if (liveNodes.isEmpty()) {
      return znRecord;
    }

    if (clusterData.getSimpleCapacitySet() == null) {
      logger.warn("No capacity set for resource: " + _resourceName);
      return znRecord;
    }

    // Sort the assignable nodes by id
    List<CapacityNode> assignableNodes = new ArrayList<>(clusterData.getSimpleCapacitySet());
    assignableNodes.sort(Comparator.comparing(CapacityNode::getId));

    //  Populate existing preference list given current mapping
    Map<String, List<String>> currentPreferenceListMap =
        populateCurrentPreferenceListMap(currentMapping, assignableNodes);

    // Assign partitions to node by order.
    for (int i = 0, index = 0; i < _partitions.size(); i++) {
      // Skip the partition assignment if it's already assigned with valid nodes before
      if (currentPreferenceListMap.containsKey(_partitions.get(i))) {
        znRecord.setListField(_partitions.get(i), currentPreferenceListMap.get(_partitions.get(i)));
        continue;
      }
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

  /**
   * Populates the node usage information based on the current mapping of partitions to nodes.
   * This method updates the usage information for each node based on the current assignments
   * of partitions and also returns the preference list for each partition.
   *
   * @param currentMapping A map where the key is a partition and the value is another map that
   *                       represents the current state of the partition on each node. The inner
   *                       map's key is the node ID, and the value is the state.
   * @param assignableNodes A list of {@link CapacityNode} objects representing the nodes that can
   *                        be assigned partitions. The capacity and usage of these nodes will be
   *                        updated based on the current mapping.
   * @return A map where the key is a partition and the value is a list of node IDs that currently
   *         host that partition, representing the current preference list for each partition.
   *         This map will be used later to lookup and skip the assignment computation if assignment
   *         is already done before
   */
  private Map<String, List<String>> populateCurrentPreferenceListMap(
      final Map<String, Map<String, String>> currentMapping,
      final List<CapacityNode> assignableNodes) {
    Map<String, List<String>> currentPreferenceListMap = new HashMap<>();
    // Convert the assignableNodes to map for quick lookup
    Map<String, CapacityNode> assignableNodeMap =
        assignableNodes.stream().collect(Collectors.toMap(CapacityNode::getId, node -> node));
    if (currentMapping != null) {
      for (Map.Entry<String, Map<String, String>> entry : currentMapping.entrySet()) {
        String partition = entry.getKey();
        Map<String, String> nodeStateMap = entry.getValue();
        // Set the preference list iff:
        // 1. The state map in the assignment matches with state model &&
        // 2. The assignment can be fulfilled by the give nodes
        if (isValidStateMap(nodeStateMap) && isValidNodeAssignment(partition, nodeStateMap.keySet(),
            assignableNodeMap)) {
          currentPreferenceListMap.put(partition, new ArrayList<>(nodeStateMap.keySet()));
        }
      }
    }
    return currentPreferenceListMap;
  }

  /**
   * Validates whether the provided state mapping is valid according to the defined state model.
   *
   * This method checks if the actual state mapping of nodes matches the expected number of replicas
   * for each state as defined in the state model.
   *
   * @param actualMapping A map representing the actual state mapping where the key is the node ID and the value is the state.
   * @return {@code true} if the actual mapping is valid, meaning all states are correctly assigned and match the expected counts; {@code false} otherwise.
   */
  private boolean isValidStateMap(final Map<String, String> actualMapping) {
    // If the total count didn't match, it's considered as invalid
    if (countStateReplicas() != actualMapping.size()) {
      return false;
    }

    Map<String, Integer> tmpStates = new HashMap<>(_states);
    for (String state : actualMapping.values()) {
      Integer count = tmpStates.get(state);
      if (count != null && count > 0) {
        tmpStates.put(state, count - 1);
      }
    }

    return tmpStates.values().stream().allMatch(count -> count == 0);
  }

  /**
   * Validates whether the specified nodes can accommodate the assignment of a given partition for a resource.
   *
   * This method checks if each node in the provided set of node IDs can add the specified partition
   * to the resource. A node is considered valid for the partition assignment if it either already has
   * the partition or can accommodate it without exceeding its capacity.
   *
   * @param partition The partition to be assigned.
   * @param nodeIds A set of node IDs representing the nodes to be checked for the partition assignment.
   * @param assignableNodeMap A map of node IDs to {@link CapacityNode} objects representing the nodes that can potentially handle the partition.
   * @return {@code true} if all nodes in the set can accommodate the partition; {@code false} otherwise.
   */
  private boolean isValidNodeAssignment(final String partition, final Set<String> nodeIds,
      final Map<String, CapacityNode> assignableNodeMap) {
    for (String nodeId : nodeIds) {
      CapacityNode node = assignableNodeMap.get(nodeId);
      // Return invalid when following cases:
      // 1. Node is not in assignableNodeMap
      // 2. Node doesn't hold current partition and we cannot assign current partition to the node
      if (node == null || (!node.hasPartition(_resourceName, partition) && !node.canAdd(
          _resourceName, partition))) {
        return false;
      }
    }
    return true;
  }
}
