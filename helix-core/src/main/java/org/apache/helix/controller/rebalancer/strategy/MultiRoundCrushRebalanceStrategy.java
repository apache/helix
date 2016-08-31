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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.strategy.crushMapping.CRUSHPlacementAlgorithm;
import org.apache.helix.util.JenkinsHash;
import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.controller.rebalancer.topology.Topology;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.model.InstanceConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Multi-round CRUSH partition mapping strategy.
 * This gives more even partition distribution in case of small number of partitions,
 * but number of partitions to be reshuffled during node outage could be higher than CrushRebalanceStrategy.
 */
public class MultiRoundCrushRebalanceStrategy implements RebalanceStrategy {
  private String _resourceName;
  private List<String> _partitions;
  private Topology _clusterTopo;
  private int _replicas;
  private LinkedHashMap<String, Integer> _stateCountMap;

  private final int MAX_ITERNATION = 5;

  @Override
  public void init(String resourceName, final List<String> partitions,
      final LinkedHashMap<String, Integer> states, int maximumPerNode) {
    _resourceName = resourceName;
    _partitions = partitions;
    _replicas = countStateReplicas(states);
    _stateCountMap = states;
  }

  /**
   * Compute the preference lists and (optional partition-state mapping) for the given resource.
   *
   * @param allNodes       All instances
   * @param liveNodes      List of live instances
   * @param currentMapping current replica mapping
   * @param clusterData    cluster data
   * @return
   * @throws HelixException if a map can not be found
   */
  @Override public ZNRecord computePartitionAssignment(final List<String> allNodes,
      final List<String> liveNodes, final Map<String, Map<String, String>> currentMapping,
      ClusterDataCache clusterData) throws HelixException {
    Map<String, InstanceConfig> instanceConfigMap = clusterData.getInstanceConfigMap();
    _clusterTopo =
        new Topology(allNodes, liveNodes, instanceConfigMap, clusterData.getClusterConfig());
    Node root = _clusterTopo.getRootNode();

    Map<String, List<Node>> zoneMapping = new HashMap<String, List<Node>>();
    for (int i = 0; i < _partitions.size(); i++) {
      String partitionName = _partitions.get(i);
      long pData = partitionName.hashCode();

      // select zones for this partition
      List<Node> zones = select(root, _clusterTopo.getFaultZoneType(), pData, _replicas);
      zoneMapping.put(partitionName, zones);
    }

    /* map the position in preference list to the state */
    Map<Integer, String> idxStateMap = new HashMap<Integer, String>();
    int i = 0;
    for (Map.Entry<String, Integer> e : _stateCountMap.entrySet()) {
      String state = e.getKey();
      int count = e.getValue();
      for (int j = 0; j < count; j++) {
        idxStateMap.put(i + j, state);
      }
      i += count;
    }

    // Final mapping <partition, state> -> list(node)
    Map<String, Map<String, List<Node>>> partitionStateMapping =
        new HashMap<String, Map<String, List<Node>>>();

    for (Node zone : _clusterTopo.getFaultZones()) {
      // partition state -> list(partitions)
      LinkedHashMap<String, List<String>> statePartitionMap =
          new LinkedHashMap<String, List<String>>();
      // TODO: move this outside?
      for (Map.Entry<String, List<Node>> e : zoneMapping.entrySet()) {
        String partition = e.getKey();
        List<Node> zones = e.getValue();
        for (int k = 0; k < zones.size(); k++) {
          if (zones.get(k).equals(zone)) {
            String state = idxStateMap.get(k);
            if (!statePartitionMap.containsKey(state)) {
              statePartitionMap.put(state, new ArrayList<String>());
            }
            statePartitionMap.get(state).add(partition);
          }
        }
      }

      for (String state : _stateCountMap.keySet()) {
        List<String> partitions = statePartitionMap.get(state);
        if (partitions != null && !partitions.isEmpty()) {
          Map<String, Node> assignments = singleZoneMapping(zone, partitions);
          for (String partition : assignments.keySet()) {
            Node node = assignments.get(partition);
            if (!partitionStateMapping.containsKey(partition)) {
              partitionStateMapping.put(partition, new HashMap<String, List<Node>>());
            }
            Map<String, List<Node>> stateMapping = partitionStateMapping.get(partition);
            if (!stateMapping.containsKey(state)) {
              stateMapping.put(state, new ArrayList<Node>());
            }
            stateMapping.get(state).add(node);
          }
        }
      }
    }

    return generateZNRecord(_resourceName, _partitions, partitionStateMapping);
  }

  private ZNRecord generateZNRecord(String resource, List<String> partitions,
      Map<String, Map<String, List<Node>>> partitionStateMapping) {
    Map<String, List<String>> newPreferences = new HashMap<String, List<String>>();
    for (int i = 0; i < partitions.size(); i++) {
      String partitionName = partitions.get(i);
      Map<String, List<Node>> stateNodeMap = partitionStateMapping.get(partitionName);

      for (String state : _stateCountMap.keySet()) {
        List<Node> nodes = stateNodeMap.get(state);
        List<String> nodeList = new ArrayList<String>();
        for (int j = 0; j < nodes.size(); j++) {
          nodeList.add(nodes.get(j).getName());
        }
        if (!newPreferences.containsKey(partitionName)) {
          newPreferences.put(partitionName, new ArrayList<String>());
        }
        newPreferences.get(partitionName).addAll(nodeList);
      }
    }
    ZNRecord result = new ZNRecord(resource);
    result.setListFields(newPreferences);

    return result;
  }

  /**
   * Compute mapping of partition to node in a single zone.
   * Assumption: A partition should have only one replica at one zone.
   * Will apply CRUSH multiple times until all partitions are mostly even distributed.
   *
   * @param zone       the zone
   * @param partitions partitions to be assigned to nodes in the given zone.
   * @return partition to node mapping in this zone.
   */
  private Map<String, Node> singleZoneMapping(Node zone, List<String> partitions) {
    if (zone.isFailed() || zone.getWeight() == 0 || partitions.isEmpty()) {
      return Collections.emptyMap();
    }

    long totalWeight = zone.getWeight();
    int totalPartition = partitions.size();

    // node to all its assigned partitions.
    Map<Node, List<String>> nodePartitionsMap = new HashMap<Node, List<String>>();

    List<String> partitionsToAssign = new ArrayList<String>(partitions);
    Map<Node, List<String>> toRemovedMap = new HashMap<Node, List<String>>();

    int iteration = 0;
    Node root = zone;
    while (iteration++ < MAX_ITERNATION) {
      for (Map.Entry<Node, List<String>> e : toRemovedMap.entrySet()) {
        List<String> curAssignedPartitions = nodePartitionsMap.get(e.getKey());
        List<String> toRemoved = e.getValue();
        curAssignedPartitions.removeAll(toRemoved);
        partitionsToAssign.addAll(toRemoved);
      }

      for (String p : partitionsToAssign) {
        long pData = p.hashCode();
        List<Node> nodes = select(root, _clusterTopo.getEndNodeType(), pData, 1);
        for (Node n : nodes) {
          if (!nodePartitionsMap.containsKey(n)) {
            nodePartitionsMap.put(n, new ArrayList<String>());
          }
          nodePartitionsMap.get(n).add(p);
        }
      }

      Map<String, Integer> newNodeWeight = new HashMap<String, Integer>();
      Set<String> completedNodes = new HashSet<String>();
      for (Node node : Topology.getAllLeafNodes(zone)) {
        if (node.isFailed()) {
          completedNodes.add(node.getName());
          continue;
        }
        long weight = node.getWeight();
        double ratio = ((double) weight) / (double) totalWeight;
        int target = (int) Math.floor(ratio * totalPartition);

        List<String> assignedPatitions = nodePartitionsMap.get(node);
        int numPartitions = 0;
        if (assignedPatitions != null) {
          numPartitions = assignedPatitions.size();
        }
        if (numPartitions > target + 1) {
          int remove = numPartitions - target - 1;
          Collections.sort(partitions);
          List<String> toRemoved = new ArrayList<String>(assignedPatitions.subList(0, remove));
          toRemovedMap.put(node, toRemoved);
        }

        int missing = target - numPartitions;
        if (missing > 0) {
          newNodeWeight.put(node.getName(), missing * 10);
        } else {
          completedNodes.add(node.getName());
        }
      }

      if (newNodeWeight.isEmpty()) {
        // already converged
        break;
      } else {
        // iterate more
        root = _clusterTopo.clone(zone, newNodeWeight, completedNodes);
      }

      partitionsToAssign.clear();
    }

    Map<String, Node> partitionMap = new HashMap<String, Node>();
    for (Map.Entry<Node, List<String>> e : nodePartitionsMap.entrySet()) {
      Node n = e.getKey();
      List<String> assigned = e.getValue();
      for (String p : assigned) {
        partitionMap.put(p, n);
      }
    }

    return partitionMap;
  }

  /**
   * Number of retries for finding an appropriate instance for a replica.
   */
  private static final int MAX_RETRY = 100;
  private final JenkinsHash hashFun = new JenkinsHash();
  private CRUSHPlacementAlgorithm placementAlgorithm = new CRUSHPlacementAlgorithm();

  /**
   * For given input, select a number of children with given type.
   * The caller will either get the expected number of selected nodes as a result,
   * or an exception will be thrown.
   */
  private List<Node> select(Node topNode, String nodeType, long data, int rf)
      throws HelixException {
    List<Node> zones = new ArrayList<Node>();
    long input = data;
    int count = rf;
    int tries = 0;
    while (zones.size() < rf) {
      List<Node> selected = placementAlgorithm
          .select(topNode, input, rf, nodeType, nodeAlreadySelected(new HashSet<Node>(zones)));
      // add the racks to the selected racks
      zones.addAll(selected);
      count = rf - zones.size();
      if (count > 0) {
        input = hashFun.hash(input); // create a different hash value for retrying
        tries++;
        if (tries >= MAX_RETRY) {
          throw new HelixException(
              String.format("could not find all mappings after %d tries", tries));
        }
      }
    }
    return zones;
  }

  /**
   * Use the predicate to reject already selected zones or nodes.
   */
  private Predicate<Node> nodeAlreadySelected(Set<Node> selectedNodes) {
    return Predicates.not(Predicates.in(selectedNodes));
  }

  /**
   * Counts the total number of replicas given a state-count mapping
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
