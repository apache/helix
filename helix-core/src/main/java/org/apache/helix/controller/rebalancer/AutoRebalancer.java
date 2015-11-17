package org.apache.helix.controller.rebalancer;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy.DefaultPlacementScheme;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy.ReplicaPlacementScheme;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

/**
 * This is a Rebalancer specific to full automatic mode. It is tasked with computing the ideal
 * state of a resource, fully adapting to the addition or removal of instances. This includes
 * computation of a new preference list and a partition to instance and state mapping based on the
 * computed instance preferences.
 * The input is the current assignment of partitions to instances, as well as existing instance
 * preferences, if any.
 * The output is a preference list and a mapping based on that preference list, i.e. partition p
 * has a replica on node k with state s.
 */
public class AutoRebalancer implements Rebalancer, MappingCalculator {
  // These should be final, but are initialized in init rather than a constructor
  private HelixManager _manager;
  private AutoRebalanceStrategy _algorithm;

  private static final Logger LOG = Logger.getLogger(AutoRebalancer.class);

  @Override
  public void init(HelixManager manager) {
    this._manager = manager;
    this._algorithm = null;
  }

  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
    List<String> partitions = new ArrayList<String>(currentIdealState.getPartitionSet());
    String stateModelName = currentIdealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = clusterData.getStateModelDef(stateModelName);
    Map<String, LiveInstance> liveInstance = clusterData.getLiveInstances();
    String replicas = currentIdealState.getReplicas();

    LinkedHashMap<String, Integer> stateCountMap = new LinkedHashMap<String, Integer>();
    stateCountMap = stateCount(stateModelDef, liveInstance.size(), Integer.parseInt(replicas));
    List<String> liveNodes = new ArrayList<String>(liveInstance.keySet());
    List<String> allNodes = new ArrayList<String>(clusterData.getInstanceConfigMap().keySet());
    Map<String, Map<String, String>> currentMapping =
        currentMapping(currentStateOutput, resourceName, partitions, stateCountMap);

    // If there are nodes tagged with resource name, use only those nodes
    Set<String> taggedNodes = new HashSet<String>();
    Set<String> taggedLiveNodes = new HashSet<String>();
    if (currentIdealState.getInstanceGroupTag() != null) {
      for (String instanceName : allNodes) {
        if (clusterData.getInstanceConfigMap().get(instanceName)
            .containsTag(currentIdealState.getInstanceGroupTag())) {
          taggedNodes.add(instanceName);
          if (liveInstance.containsKey(instanceName)) {
            taggedLiveNodes.add(instanceName);
          }
        }
      }
      if (!taggedLiveNodes.isEmpty()) {
        // live nodes exist that have this tag
        if (LOG.isInfoEnabled()) {
          LOG.info("found the following participants with tag "
              + currentIdealState.getInstanceGroupTag() + " for " + resourceName + ": "
              + taggedLiveNodes);
        }
      } else if (taggedNodes.isEmpty()) {
        // no live nodes and no configured nodes have this tag
        LOG.warn("Resource " + resourceName + " has tag " + currentIdealState.getInstanceGroupTag()
            + " but no configured participants have this tag");
      } else {
        // configured nodes have this tag, but no live nodes have this tag
        LOG.warn("Resource " + resourceName + " has tag " + currentIdealState.getInstanceGroupTag()
            + " but no live participants have this tag");
      }
      allNodes = new ArrayList<String>(taggedNodes);
      liveNodes = new ArrayList<String>(taggedLiveNodes);
    }

    // sort node lists to ensure consistent preferred assignments
    Collections.sort(allNodes);
    Collections.sort(liveNodes);

    int maxPartition = currentIdealState.getMaxPartitionsPerInstance();

    ReplicaPlacementScheme placementScheme = new DefaultPlacementScheme();
    placementScheme.init(_manager);
    _algorithm =
        new AutoRebalanceStrategy(resourceName, partitions, stateCountMap, maxPartition,
            placementScheme);
    ZNRecord newMapping =
        _algorithm.computePartitionAssignment(liveNodes, currentMapping, allNodes);

    if (LOG.isDebugEnabled()) {
      LOG.debug("currentMapping: " + currentMapping);
      LOG.debug("stateCountMap: " + stateCountMap);
      LOG.debug("liveNodes: " + liveNodes);
      LOG.debug("allNodes: " + allNodes);
      LOG.debug("maxPartition: " + maxPartition);
      LOG.debug("newMapping: " + newMapping);
    }

    IdealState newIdealState = new IdealState(resourceName);
    newIdealState.getRecord().setSimpleFields(currentIdealState.getRecord().getSimpleFields());
    newIdealState.setRebalanceMode(RebalanceMode.FULL_AUTO);
    newIdealState.getRecord().setListFields(newMapping.getListFields());


    boolean preferenceListChanged = false;
    for (String partition : partitions) {
      List<String> oldList = currentIdealState.getPreferenceList(partition);
      List<String> newList = newIdealState.getPreferenceList(partition);
      if (newList != null && !newList.isEmpty() && !newList.equals(oldList)) {
        preferenceListChanged = true;
        break;
      }
    }
    if (preferenceListChanged) {
      HelixDataAccessor dataAccessor = _manager.getHelixDataAccessor();
      PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
      currentIdealState.getRecord().setListFields(newIdealState.getRecord().getListFields());
      dataAccessor.setProperty(keyBuilder.idealStates(resourceName), currentIdealState);
    }


    return newIdealState;
  }

  /**
   * @return state count map: state->count
   */
  public static LinkedHashMap<String, Integer> stateCount(StateModelDefinition stateModelDef,
      int liveNodesNb, int totalReplicas) {
    LinkedHashMap<String, Integer> stateCountMap = new LinkedHashMap<String, Integer>();
    List<String> statesPriorityList = stateModelDef.getStatesPriorityList();

    int replicas = totalReplicas;
    for (String state : statesPriorityList) {
      String num = stateModelDef.getNumInstancesPerState(state);
      if ("N".equals(num)) {
        stateCountMap.put(state, liveNodesNb);
      } else if ("R".equals(num)) {
        // wait until we get the counts for all other states
        continue;
      } else {
        int stateCount = -1;
        try {
          stateCount = Integer.parseInt(num);
        } catch (Exception e) {
          // LOG.error("Invalid count for state: " + state + ", count: " + num +
          // ", use -1 instead");
        }

        if (stateCount > 0) {
          stateCountMap.put(state, stateCount);
          replicas -= stateCount;
        }
      }
    }

    // get state count for R
    for (String state : statesPriorityList) {
      String num = stateModelDef.getNumInstancesPerState(state);
      if ("R".equals(num)) {
        stateCountMap.put(state, replicas);
        // should have at most one state using R
        break;
      }
    }
    return stateCountMap;
  }

  private Map<String, Map<String, String>> currentMapping(CurrentStateOutput currentStateOutput,
      String resourceName, List<String> partitions, Map<String, Integer> stateCountMap) {

    Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();

    for (String partition : partitions) {
      Map<String, String> curStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, new Partition(partition));
      map.put(partition, new HashMap<String, String>());
      for (String node : curStateMap.keySet()) {
        String state = curStateMap.get(node);
        map.get(partition).put(node, state);
      }

      Map<String, String> pendingStateMap =
          currentStateOutput.getPendingStateMap(resourceName, new Partition(partition));
      for (String node : pendingStateMap.keySet()) {
        String state = pendingStateMap.get(node);
        map.get(partition).put(node, state);
      }
    }
    return map;
  }

  @Override
  public ResourceAssignment computeBestPossiblePartitionState(ClusterDataCache cache,
      IdealState idealState, Resource resource, CurrentStateOutput currentStateOutput) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing resource:" + resource.getResourceName());
    }
    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    ResourceAssignment partitionMapping = new ResourceAssignment(resource.getResourceName());
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resource.getResourceName(), partition);
      Set<String> disabledInstancesForPartition =
          cache.getDisabledInstancesForPartition(partition.toString());
      List<String> preferenceList =
          ConstraintBasedAssignment.getPreferenceList(cache, partition, idealState, stateModelDef);
      Map<String, String> bestStateForPartition =
          ConstraintBasedAssignment.computeAutoBestStateForPartition(cache, stateModelDef,
              preferenceList, currentStateMap, disabledInstancesForPartition,
              idealState.isEnabled());
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    return partitionMapping;
  }
}
