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
import java.util.HashMap;
import java.util.HashSet;
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

public class StickyRebalanceStrategy implements RebalanceStrategy<ResourceControllerDataProvider> {
  private static final Logger logger = LoggerFactory.getLogger(StickyRebalanceStrategy.class);
  private String _resourceName;
  private List<String> _partitions;
  private LinkedHashMap<String, Integer> _states;
  private int _statesReplicaCount;

  public StickyRebalanceStrategy() {
  }

  @Override
  public void init(String resourceName, final List<String> partitions,
      final LinkedHashMap<String, Integer> states, int maximumPerNode) {
    _resourceName = resourceName;
    _partitions = partitions;
    _states = states;
    if (_states != null) {
      _statesReplicaCount = _states.values().stream().mapToInt(Integer::intValue).sum();
    }
  }

  @Override
  public ZNRecord computePartitionAssignment(final List<String> allNodes,
      final List<String> liveNodes, final Map<String, Map<String, String>> currentMapping,
      ResourceControllerDataProvider clusterData) {
    ZNRecord znRecord = new ZNRecord(_resourceName);
    if (liveNodes.isEmpty()) {
      return znRecord;
    }

    if (clusterData.getSimpleCapacitySet() == null) {
      logger.warn("No capacity set for resource: {}", _resourceName);
      return znRecord;
    }

    // Filter out the nodes if not in the liveNodes parameter
    // Note the liveNodes parameter here might be processed within the rebalancer, e.g. filter based on tags
    Set<CapacityNode> assignableNodeSet = new HashSet<>(clusterData.getSimpleCapacitySet());
    Set<String> liveNodesSet = new HashSet<>(liveNodes);
    assignableNodeSet.removeIf(n -> !liveNodesSet.contains(n.getInstanceName()));

    // Convert the assignableNodes to map for quick lookup
    Map<String, CapacityNode> assignableNodeMap = assignableNodeSet.stream()
        .collect(Collectors.toMap(CapacityNode::getInstanceName, node -> node));

    //  Populate valid state map given current mapping
    Map<String, Set<String>> stateMap =
        populateValidAssignmentMapFromCurrentMapping(currentMapping, assignableNodeMap);

    if (logger.isDebugEnabled()) {
      logger.debug("currentMapping: {}", currentMapping);
      logger.debug("stateMap: {}", stateMap);
    }

    // Sort the assignable nodes by id
    List<CapacityNode> assignableNodeList = assignableNodeSet.stream().sorted()
            .collect(Collectors.toList());

    // Assign partitions to node by order.
    for (int i = 0, index = 0; i < _partitions.size(); i++) {
      int startIndex = index;
      Map<String, Integer> currentFaultZoneCountMap = new HashMap<>();
      int remainingReplica = _statesReplicaCount;
      if (stateMap.containsKey(_partitions.get(i))) {
        Set<String> existingReplicas = stateMap.get(_partitions.get(i));
        remainingReplica = remainingReplica - existingReplicas.size();
        for (String instanceName : existingReplicas) {
          String faultZone = assignableNodeMap.get(instanceName).getFaultZone();
          currentFaultZoneCountMap.put(faultZone,
              currentFaultZoneCountMap.getOrDefault(faultZone, 0) + 1);
        }
      }
      for (int j = 0; j < remainingReplica; j++) {
        while (index - startIndex < assignableNodeList.size()) {
          CapacityNode node = assignableNodeList.get(index++ % assignableNodeList.size());
          if (this.canAdd(node, _partitions.get(i), currentFaultZoneCountMap)) {
            stateMap.computeIfAbsent(_partitions.get(i), m -> new HashSet<>())
                .add(node.getInstanceName());
            if (node.getFaultZone() != null) {
              currentFaultZoneCountMap.put(node.getFaultZone(),
                  currentFaultZoneCountMap.getOrDefault(node.getFaultZone(), 0) + 1);
            }
            break;
          }
        }

        if (index - startIndex >= assignableNodeList.size()) {
          // If the all nodes have been tried out, then no node can be assigned.
          logger.warn("No enough assignable nodes for resource: {}", _resourceName);
        }
      }
    }
    for (Map.Entry<String, Set<String>> entry : stateMap.entrySet()) {
      znRecord.setListField(entry.getKey(), new ArrayList<>(entry.getValue()));
    }
    if (logger.isDebugEnabled()) {
      logger.debug("znRecord: {}", znRecord);
    }

    return znRecord;
  }

  /**
   * Populates a valid state map from the current mapping, filtering out invalid nodes.
   *
   * @param currentMapping   the current mapping of partitions to node states
   * @param assignableNodeMap  the map of instance name -> nodes that can be assigned
   * @return a map of partitions to valid nodes
   */
  private Map<String, Set<String>> populateValidAssignmentMapFromCurrentMapping(
      final Map<String, Map<String, String>> currentMapping,
      final Map<String, CapacityNode> assignableNodeMap) {
    Map<String, Set<String>> validAssignmentMap = new HashMap<>();
    if (currentMapping != null) {
      for (Map.Entry<String, Map<String, String>> entry : currentMapping.entrySet()) {
        String partition = entry.getKey();
        Map<String, String> currentNodeStateMap = new HashMap<>(entry.getValue());
        // Filter out invalid node assignment
        currentNodeStateMap.entrySet()
            .removeIf(e -> !isValidNodeAssignment(partition, e.getKey(), assignableNodeMap));

        validAssignmentMap.put(partition, new HashSet<>(currentNodeStateMap.keySet()));
      }
    }
    return validAssignmentMap;
  }

  /**
   * Checks if a node assignment is valid for a given partition.
   *
   * @param partition           the partition to be assigned
   * @param nodeId              the ID of the node to be checked
   * @param assignableNodeMap   the map of node IDs to CapacityNode objects
   * @return true if the node is valid for the assignment, false otherwise
   */
  private boolean isValidNodeAssignment(final String partition, final String nodeId,
      final Map<String, CapacityNode> assignableNodeMap) {
    CapacityNode node = assignableNodeMap.get(nodeId);
    // Return valid when following conditions match:
    // 1. Node is in assignableNodeMap
    // 2. Node hold current partition or we can assign current partition to the node
    return node != null && (node.hasPartition(_resourceName, partition) || node.canAdd(
        _resourceName, partition));
  }

  /**
   * Checks if it's valid to assign the partition to node
   *
   * @param node           node to assign partition
   * @param partition      partition name
   * @param currentFaultZoneCountMap   the map of fault zones -> count
   * @return true if it's valid to assign the partition to node, false otherwise
   */
  protected boolean canAdd(CapacityNode node, String partition,
      Map<String, Integer> currentFaultZoneCountMap) {
    // Valid assignment when following conditions match:
    // 1. Replica is not within the same fault zones of other replicas
    // 2. Node has capacity to hold the replica
    return !currentFaultZoneCountMap.containsKey(node.getFaultZone()) && node.canAdd(_resourceName,
        partition);
  }
}

