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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.rebalancer.strategy.crushMapping.CardDealingAdjustmentAlgorithmV2;
import org.apache.helix.controller.rebalancer.strategy.crushMapping.ConsistentHashingAdjustmentAlgorithm;
import org.apache.helix.controller.rebalancer.topology.InstanceNode;
import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.controller.rebalancer.topology.Topology;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class of Forced Even Assignment Patched Algorithm.
 * This class contains common logic that re-calculate assignment based on a result calculated by the base algorithm.
 * The target of this patching step is more even partition distribution, but number of partitions to be reshuffled during node outage could be higher than the base algorithm.
 */
public abstract class AbstractEvenDistributionRebalanceStrategy implements RebalanceStrategy {
  private static final Logger _logger =
      LoggerFactory.getLogger(AbstractEvenDistributionRebalanceStrategy.class);
  protected String _resourceName;
  protected int _replica;

  protected abstract RebalanceStrategy getBaseRebalanceStrategy();

  protected CardDealingAdjustmentAlgorithmV2 getCardDealingAlgorithm(Topology topology) {
    // by default, minimize the movement when calculating for evenness.
    return new CardDealingAdjustmentAlgorithmV2(topology, _replica,
        CardDealingAdjustmentAlgorithmV2.Mode.MINIMIZE_MOVEMENT);
  }

  @Override
  public void init(String resourceName, final List<String> partitions,
      final LinkedHashMap<String, Integer> states, int maximumPerNode) {
    _resourceName = resourceName;
    getBaseRebalanceStrategy().init(resourceName, partitions, states, maximumPerNode);
    _replica = countStateReplicas(states);
  }

  /**
   * Force uniform distribution based on the parent strategy class's calculation result.
   *
   * @param allNodes       All instances
   * @param liveNodes      List of live instances
   * @param currentMapping current replica mapping
   * @param clusterData    cluster data
   * @return
   * @throws HelixException
   */
  @Override
  public ZNRecord computePartitionAssignment(final List<String> allNodes,
      final List<String> liveNodes, final Map<String, Map<String, String>> currentMapping,
      ClusterDataCache clusterData) throws HelixException {
    // Round 1: Calculate mapping using the base strategy.
    // Note to use all nodes for minimizing the influence of live node changes to mapping.
    ZNRecord origAssignment = getBaseRebalanceStrategy()
        .computePartitionAssignment(allNodes, allNodes, currentMapping, clusterData);
    Map<String, List<String>> origPartitionMap = origAssignment.getListFields();

    // For logging only
    String eventId = clusterData.getEventId();

    // Try to re-assign if the original map is not empty
    if (!origPartitionMap.isEmpty()) {
      Map<String, List<Node>> finalPartitionMap = null;
      Topology allNodeTopo = new Topology(allNodes, allNodes, clusterData.getInstanceConfigMap(),
          clusterData.getClusterConfig());
      // Transform current assignment to instance->partitions map, and get total partitions
      Map<Node, List<String>> nodeToPartitionMap =
          convertPartitionMap(origPartitionMap, allNodeTopo);
      // Round 2: Rebalance mapping using card dealing algorithm. For ensuring evenness distribution.
      CardDealingAdjustmentAlgorithmV2 cardDealer = getCardDealingAlgorithm(allNodeTopo);
      if (cardDealer.computeMapping(nodeToPartitionMap, _resourceName.hashCode())) {
        // Round 3: Reorder preference Lists to ensure participants' orders (so as the states) are uniform.
        finalPartitionMap = shufflePreferenceList(nodeToPartitionMap);
        if (!liveNodes.containsAll(allNodes)) {
          try {
            // Round 4: Re-mapping the partitions on non-live nodes using consistent hashing for reducing movement.
            ConsistentHashingAdjustmentAlgorithm hashPlacement =
                new ConsistentHashingAdjustmentAlgorithm(allNodeTopo, liveNodes);
            if (hashPlacement.computeMapping(nodeToPartitionMap, _resourceName.hashCode())) {
              // Since mapping is changed by hashPlacement, need to adjust nodes order.
              Map<String, List<Node>> adjustedPartitionMap =
                  convertAssignment(nodeToPartitionMap);
              for (String partition : adjustedPartitionMap.keySet()) {
                List<Node> preSelectedList = finalPartitionMap.get(partition);
                Set<Node> adjustedNodeList =
                    new HashSet<>(adjustedPartitionMap.get(partition));
                List<Node> finalNodeList = adjustedPartitionMap.get(partition);
                int index = 0;
                // 1. Add the ones in pre-selected node list first, in order
                for (Node node : preSelectedList) {
                  if (adjustedNodeList.remove(node)) {
                    finalNodeList.set(index++, node);
                  }
                }
                // 2. Add the rest of nodes to the map
                for (Node node : adjustedNodeList) {
                  finalNodeList.set(index++, node);
                }
              }
              finalPartitionMap = adjustedPartitionMap;
            } else {
              // Adjustment failed, the final partition map is not valid
              finalPartitionMap = null;
            }
          } catch (ExecutionException e) {
            LogUtil.logError(_logger, eventId,
                "Failed to perform consistent hashing partition assigner.", e);
            finalPartitionMap = null;
          }
        }
      }

      if (null != finalPartitionMap) {
        ZNRecord result = new ZNRecord(_resourceName);
        Map<String, List<String>> resultPartitionMap = new HashMap<>();
        for (String partitionName : finalPartitionMap.keySet()) {
          List<String> instanceNames = new ArrayList<>();
          for (Node node : finalPartitionMap.get(partitionName)) {
            if (node instanceof InstanceNode) {
              instanceNames.add(((InstanceNode) node).getInstanceName());
            } else {
              LogUtil.logError(_logger, eventId,
                  String.format("Selected node is not associated with an instance: %s", node));
            }
          }
          resultPartitionMap.put(partitionName, instanceNames);
        }
        result.setListFields(resultPartitionMap);
        return result;
      }
    }

    // Force even is not possible, fallback to use default strategy
    if (_logger.isDebugEnabled()) {
      LogUtil.logDebug(_logger, eventId,
          "Force even distribution is not possible, using the default strategy: "
              + getBaseRebalanceStrategy().getClass().getSimpleName());
    }

    if (liveNodes.equals(allNodes)) {
      return origAssignment;
    } else {
      // need to re-calculate since node list is different.
      return getBaseRebalanceStrategy()
          .computePartitionAssignment(allNodes, liveNodes, currentMapping, clusterData);
    }
  }

  // Best effort to shuffle preference lists for all partitions for uniform distribution regarding the top state.
  private Map<String, List<Node>> shufflePreferenceList(
      Map<Node, List<String>> nodeToPartitionMap) {
    final Map<String, List<Node>> partitionMap = convertAssignment(nodeToPartitionMap);
    // evaluate node's order according to:
    // 1. their potential top state replicas count (less count, higher priority)
    // 2. their assigned top state replicas (less top state replica, higher priority)
    final Map<Node, Integer> nodeScores = new HashMap<>();
    for (Node node : nodeToPartitionMap.keySet()) {
      // Init with the potential replicas count
      nodeScores.put(node, nodeToPartitionMap.get(node).size());
    }
    for (final String partition : partitionMap.keySet()) {
      List<Node> nodes = partitionMap.get(partition);
      // order according to score
      Collections.sort(nodes, new Comparator<Node>() {
        @Override
        public int compare(Node o1, Node o2) {
          int o1Score = nodeScores.get(o1);
          int o2Score = nodeScores.get(o2);
          if (o1Score == o2Score) {
            return new Integer((partition + o1.getName()).hashCode())
                .compareTo((partition + o2.getName()).hashCode());
          } else {
            return o1Score - o2Score;
          }
        }
      });
      // After assignment, the nodes has less potential top states
      for (int i = 0; i < nodes.size(); i++) {
        Node node = nodes.get(i);
        nodeScores.put(node, nodeScores.get(node) - 1 + (i == 0 ? (int) Math.pow(_replica, 2) : 0));
      }
    }
    return partitionMap;
  }

  // Convert the map from <key, list of values> to a new map <original value, list of related keys>
  private Map<String, List<Node>> convertAssignment(
      Map<Node, List<String>> assignment) {
    Map<String, List<Node>> resultMap = new HashMap<>();
    for (Node instance : assignment.keySet()) {
      for (String partitionName : assignment.get(instance)) {
        if (!resultMap.containsKey(partitionName)) {
          resultMap.put(partitionName, new ArrayList<Node>());
        }
        resultMap.get(partitionName).add(instance);
      }
    }
    return resultMap;
  }

  // Convert the map from <Partition Name, List<instance names>> to a new map <InstanceNode, List<Partition Name>>
  private Map<Node, List<String>> convertPartitionMap(Map<String, List<String>> originalMap,
      Topology topology) {
    Map<Node, List<String>> resultMap = new HashMap<>();
    Map<String, Node> instanceMap = new HashMap<>();
    for (Node node : Topology.getAllLeafNodes(topology.getRootNode())) {
      if (node instanceof InstanceNode) {
        InstanceNode insNode = (InstanceNode) node;
        instanceMap.put(insNode.getInstanceName(), insNode);
      }
    }

    for (String partition : originalMap.keySet()) {
      for (String instanceName : originalMap.get(partition)) {
        Node insNode = instanceMap.get(instanceName);
        if (insNode != null) {
          if (!resultMap.containsKey(insNode)) {
            resultMap.put(insNode, new ArrayList<String>());
          }
          resultMap.get(insNode).add(partition);
        }
      }
    }
    return resultMap;
  }

  /**
   * Counts the total number of replicas given a state-count mapping
   *
   * @return
   */
  private int countStateReplicas(Map<String, Integer> stateCountMap) {
    int total = 0;
    for (Integer count : stateCountMap.values()) {
      total += count;
    }
    return total;
  }
}
