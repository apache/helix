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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.helix.HelixException;
import org.apache.helix.controller.rebalancer.strategy.crushMapping.Selector;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.strategy.crushMapping.CRUSHPlacementAlgorithm;
import org.apache.helix.controller.rebalancer.topology.InstanceNode;
import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.controller.rebalancer.topology.Topology;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.util.JenkinsHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CRUSH-based partition mapping strategy.
 */
public class CrushRebalanceStrategy implements RebalanceStrategy<ResourceControllerDataProvider> {
  private static final Logger Log = LoggerFactory.getLogger(CrushRebalanceStrategy.class.getName());

  private String _resourceName;
  private List<String> _partitions;
  private Topology _clusterTopo;
  private int _replicas;

  /**
   * Number of retries for finding an appropriate instance for a replica.
   */
  private static final int MAX_RETRY = 10;
  private final JenkinsHash hashFun = new JenkinsHash();
  private CRUSHPlacementAlgorithm placementAlgorithm;

  public CrushRebalanceStrategy() {
    this(Selector.StrawBucket.STRAW);
  }

  public CrushRebalanceStrategy(Selector.StrawBucket strawBucket) {
    placementAlgorithm = new CRUSHPlacementAlgorithm(strawBucket);
  }

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
      ResourceControllerDataProvider clusterData) throws HelixException {
    Map<String, InstanceConfig> instanceConfigMap = clusterData.getAssignableInstanceConfigMap();
    _clusterTopo =
        new Topology(allNodes, liveNodes, instanceConfigMap, clusterData.getClusterConfig(), true);
    Node topNode = _clusterTopo.getRootNode();

    // for log only
    String eventId = clusterData.getClusterEventId();

    Map<String, List<String>> newPreferences = new HashMap<>();
    for (int i = 0; i < _partitions.size(); i++) {
      String partitionName = _partitions.get(i);
      long data = partitionName.hashCode();

      // apply the placement rules
      List<Node> selected;
      try {
        selected = select(topNode, data, _replicas, eventId);
      } catch (IllegalStateException e) {
        String errorMessage = String
            .format("Could not select enough number of nodes. %s partition %s, required %d",
                _resourceName, partitionName, _replicas);
        throw new HelixException(errorMessage, e);
      }

      if (selected.size() < _replicas) {
        LogUtil.logError(Log, eventId, String
            .format("Can not find enough node for resource %s partition %s, required %d, find %d",
                _resourceName, partitionName, _replicas, selected.size()));
      }

      List<String> nodeList = new ArrayList<>();
      for (int j = 0; j < selected.size(); j++) {
        Node selectedNode = selected.get(j);
        if (selectedNode instanceof InstanceNode) {
          nodeList.add(((InstanceNode) selectedNode).getInstanceName());
        } else {
          LogUtil.logError(Log, eventId,
              "Selected node is not associated with an instance: " + selectedNode.toString());
        }
      }

      newPreferences.put(partitionName, nodeList);
    }

    ZNRecord result = new ZNRecord(_resourceName);
    result.setListFields(newPreferences);

    return result;
  }

  /**
   * Enforce isolation on the specified fault zone.
   * The caller will try to get the expected number of selected nodes as a result,
   * if no enough nodes can be found, could return any number of nodes than required.
   */
  private List<Node> select(Node topNode, long data, int rf, String eventId)
      throws HelixException {
    List<Node> nodes = new ArrayList<>(rf);
    Set<Node> selectedZones = new HashSet<>();
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
          LogUtil.logError(Log, eventId,
              String.format("Could not find all mappings after %d tries", tries));
          break;
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
      List<Node> zones = placementAlgorithm.select(topNode, input, rf, zoneType,
          nodeAlreadySelected(selectedZones));
      // add the racks to the selected racks
      selectedZones.addAll(zones);
      // pick one end node from each fault zone.
      for (Node zone : zones) {
        List<Node> endNode = placementAlgorithm.select(zone, input, 1, endNodeType);
        selectedNodes.addAll(endNode);
      }
    } else {
      // pick end node directly
      List<Node> nodes = placementAlgorithm
          .select(topNode, input, rf, endNodeType, nodeAlreadySelected(new HashSet(selectedNodes)));
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
