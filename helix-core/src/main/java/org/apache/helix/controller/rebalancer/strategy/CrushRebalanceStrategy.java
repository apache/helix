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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CRUSH-based partition mapping strategy.
 */
public class CrushRebalanceStrategy implements RebalanceStrategy {
  private String _resourceName;
  private List<String> _partitions;
  private Topology _clusterTopo;
  private int _replicas;

  @Override
  public void init(String resourceName, final List<String> partitions,
      final LinkedHashMap<String, Integer> states, int maximumPerNode) {
    _resourceName = resourceName;
    _partitions = partitions;
    _replicas = countStateReplicas(states);
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
  @Override
  public ZNRecord computePartitionAssignment(final List<String> allNodes,
      final List<String> liveNodes, final Map<String, Map<String, String>> currentMapping,
      ClusterDataCache clusterData) throws HelixException {
    Map<String, InstanceConfig> instanceConfigMap = clusterData.getInstanceConfigMap();
    _clusterTopo =
        new Topology(allNodes, liveNodes, instanceConfigMap, clusterData.getClusterConfig());
    Node topNode = _clusterTopo.getRootNode();

    Map<String, List<String>> newPreferences = new HashMap<String, List<String>>();
    for (int i = 0; i < _partitions.size(); i++) {
      String partitionName = _partitions.get(i);
      long data = partitionName.hashCode();

      // apply the placement rules
      List<Node> selected = select(topNode, data, _replicas);

      List<String> nodeList = new ArrayList<String>();
      for (int j = 0; j < selected.size(); j++) {
        nodeList.add(selected.get(j).getName());
      }

      newPreferences.put(partitionName, nodeList);
    }

    ZNRecord result = new ZNRecord(_resourceName);
    result.setListFields(newPreferences);

    return result;
  }

  /**
   * Number of retries for finding an appropriate instance for a replica.
   */
  private static final int MAX_RETRY = 100;
  private final JenkinsHash hashFun = new JenkinsHash();
  private CRUSHPlacementAlgorithm placementAlgorithm = new CRUSHPlacementAlgorithm();

  /**
   * Enforce isolation on the specified fault zone.
   * The caller will either get the expected number of selected nodes as a result, or an exception will be thrown.
   */
  private List<Node> select(Node topNode, long data, int rf)
      throws HelixException {
    List<Node> nodes = new ArrayList<Node>(rf);
    Set<Node> selectedZones = new HashSet<Node>();
    long input = data;
    int count = rf;
    int tries = 0;
    while (nodes.size() < rf) {
      doSelect(topNode, input, count, nodes, selectedZones);
      count = rf - nodes.size();
      if (count > 0) {
        input = hashFun.hash(input); // create a different hash value for retrying
        tries++;
        if (tries >= MAX_RETRY) {
          throw new HelixException(
              String.format("could not find all mappings after %d tries", tries));
        }
      }
    }
    return nodes;
  }

  private void doSelect(Node topNode, long input, int rf, List<Node> selectedNodes,
      Set<Node> selectedZones) {
    String zoneType = _clusterTopo.getFaultZoneType();
    String endNodeType = _clusterTopo.getEndNodeType();

    if (!zoneType.equals(endNodeType)) {
      // pick fault zones first
      List<Node> zones = placementAlgorithm
          .select(topNode, input, rf, zoneType, nodeAlreadySelected(selectedZones));
      // add the racks to the selected racks
      selectedZones.addAll(zones);
      // pick one end node from each fault zone.
      for (Node zone : zones) {
        List<Node> endNode = placementAlgorithm.select(zone, input, 1, endNodeType);
        selectedNodes.addAll(endNode);
      }
    } else {
      // pick end node directly
      List<Node> nodes = placementAlgorithm.select(topNode, input, rf, endNodeType,
          nodeAlreadySelected(new HashSet(selectedNodes)));
      selectedNodes.addAll(nodes);
    }
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
