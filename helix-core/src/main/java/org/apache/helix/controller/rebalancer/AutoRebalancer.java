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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.ResourceAssignment;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy.DefaultPlacementScheme;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy.ReplicaPlacementScheme;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
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
    Map<String, Map<String, String>> currentMapping =
        currentMapping(currentStateOutput, resourceName, partitions, stateCountMap);

    List<String> allNodes = new ArrayList<String>(clusterData.getInstanceConfigMap().keySet());
    int maxPartition = currentIdealState.getMaxPartitionsPerInstance();

    if (LOG.isInfoEnabled()) {
      LOG.info("currentMapping: " + currentMapping);
      LOG.info("stateCountMap: " + stateCountMap);
      LOG.info("liveNodes: " + liveNodes);
      LOG.info("allNodes: " + allNodes);
      LOG.info("maxPartition: " + maxPartition);
    }
    ReplicaPlacementScheme placementScheme = new DefaultPlacementScheme();
    placementScheme.init(_manager);
    _algorithm =
        new AutoRebalanceStrategy(resourceName, partitions, stateCountMap, maxPartition,
            placementScheme);
    ZNRecord newMapping =
        _algorithm.computePartitionAssignment(liveNodes, currentMapping, allNodes);

    if (LOG.isInfoEnabled()) {
      LOG.info("newMapping: " + newMapping);
    }

    IdealState newIdealState = new IdealState(resourceName);
    newIdealState.getRecord().setSimpleFields(currentIdealState.getRecord().getSimpleFields());
    newIdealState.setRebalanceMode(RebalanceMode.FULL_AUTO);
    newIdealState.getRecord().setListFields(newMapping.getListFields());
    return newIdealState;
  }

  /**
   * @return state count map: state->count
   */
  private LinkedHashMap<String, Integer> stateCount(StateModelDefinition stateModelDef,
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
        if (stateCountMap.containsKey(state)) {
          map.get(partition).put(node, state);
        }
      }

      Map<String, String> pendingStateMap =
          currentStateOutput.getPendingStateMap(resourceName, new Partition(partition));
      for (String node : pendingStateMap.keySet()) {
        String state = pendingStateMap.get(node);
        if (stateCountMap.containsKey(state)) {
          map.get(partition).put(node, state);
        }
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
    calculateAutoBalancedIdealState(cache, idealState, stateModelDef);
    ResourceAssignment partitionMapping = new ResourceAssignment();
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resource.getResourceName(), partition);
      Set<String> disabledInstancesForPartition =
          cache.getDisabledInstancesForPartition(partition.toString());
      List<String> preferenceList =
          ConstraintBasedAssignment.getPreferenceList(cache, partition, idealState, stateModelDef);
      Map<String, String> bestStateForPartition =
          ConstraintBasedAssignment.computeAutoBestStateForPartition(cache, stateModelDef,
              preferenceList, currentStateMap, disabledInstancesForPartition);
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    return partitionMapping;
  }

  /**
   * Compute best state for resource in AUTO_REBALANCE ideal state mode. the algorithm
   * will make sure that the master partition are evenly distributed; Also when instances
   * are added / removed, the amount of diff in master partitions are minimized
   * @param cache
   * @param idealState
   * @param instancePreferenceList
   * @param stateModelDef
   * @param currentStateOutput
   * @return
   */
  private void calculateAutoBalancedIdealState(ClusterDataCache cache, IdealState idealState,
      StateModelDefinition stateModelDef) {
    String topStateValue = stateModelDef.getStatesPriorityList().get(0);
    Set<String> liveInstances = cache.getLiveInstances().keySet();
    Set<String> taggedInstances = new HashSet<String>();

    // If there are instances tagged with resource name, use only those instances
    if (idealState.getInstanceGroupTag() != null) {
      for (String instanceName : liveInstances) {
        if (cache.getInstanceConfigMap().get(instanceName)
            .containsTag(idealState.getInstanceGroupTag())) {
          taggedInstances.add(instanceName);
        }
      }
    }
    if (taggedInstances.size() > 0) {
      if (LOG.isInfoEnabled()) {
        LOG.info("found the following instances with tag " + idealState.getResourceName() + " "
            + taggedInstances);
      }
      liveInstances = taggedInstances;
    }
    // Obtain replica number
    int replicas = 1;
    try {
      replicas = Integer.parseInt(idealState.getReplicas());
    } catch (Exception e) {
      LOG.error("", e);
    }
    // Init for all partitions with empty list
    Map<String, List<String>> defaultListFields = new TreeMap<String, List<String>>();
    List<String> emptyList = new ArrayList<String>(0);
    for (String partition : idealState.getPartitionSet()) {
      defaultListFields.put(partition, emptyList);
    }
    idealState.getRecord().setListFields(defaultListFields);
    // Return if no live instance
    if (liveInstances.size() == 0) {
      if (LOG.isInfoEnabled()) {
        LOG.info("No live instances, return. Idealstate : " + idealState.getResourceName());
      }
      return;
    }
    Map<String, List<String>> masterAssignmentMap = new HashMap<String, List<String>>();
    for (String instanceName : liveInstances) {
      masterAssignmentMap.put(instanceName, new ArrayList<String>());
    }
    Set<String> orphanedPartitions = new HashSet<String>();
    orphanedPartitions.addAll(idealState.getPartitionSet());
    // Go through all current states and fill the assignments
    for (String liveInstanceName : liveInstances) {
      CurrentState currentState =
          cache.getCurrentState(liveInstanceName,
              cache.getLiveInstances().get(liveInstanceName).getSessionId())
              .get(idealState.getId());
      if (currentState != null) {
        Map<String, String> partitionStates = currentState.getPartitionStateMap();
        for (String partitionName : partitionStates.keySet()) {
          String state = partitionStates.get(partitionName);
          if (state.equals(topStateValue)) {
            masterAssignmentMap.get(liveInstanceName).add(partitionName);
            orphanedPartitions.remove(partitionName);
          }
        }
      }
    }
    List<String> orphanedPartitionsList = new ArrayList<String>();
    orphanedPartitionsList.addAll(orphanedPartitions);
    int maxPartitionsPerInstance = idealState.getMaxPartitionsPerInstance();
    normalizeAssignmentMap(masterAssignmentMap, orphanedPartitionsList, maxPartitionsPerInstance);
    idealState.getRecord().setListFields(
        generateListFieldFromMasterAssignment(masterAssignmentMap, replicas));
  }

  /**
   * Given the current master assignment map and the partitions not hosted, generate an
   * evenly distributed partition assignment map
   * @param masterAssignmentMap
   *          current master assignment map
   * @param orphanPartitions
   *          partitions not hosted by any instance
   * @return
   */
  private void normalizeAssignmentMap(Map<String, List<String>> masterAssignmentMap,
      List<String> orphanPartitions, int maxPartitionsPerInstance) {
    int totalPartitions = 0;
    String[] instanceNames = new String[masterAssignmentMap.size()];
    masterAssignmentMap.keySet().toArray(instanceNames);
    Arrays.sort(instanceNames);
    // Find out total partition number
    for (String key : masterAssignmentMap.keySet()) {
      totalPartitions += masterAssignmentMap.get(key).size();
      Collections.sort(masterAssignmentMap.get(key));
    }
    totalPartitions += orphanPartitions.size();

    // Find out how many partitions an instance should host
    int partitionNumber = totalPartitions / masterAssignmentMap.size();
    int leave = totalPartitions % masterAssignmentMap.size();

    for (int i = 0; i < instanceNames.length; i++) {
      int targetPartitionNo = leave > 0 ? (partitionNumber + 1) : partitionNumber;
      leave--;
      // For hosts that has more partitions, move those partitions to "orphaned"
      while (masterAssignmentMap.get(instanceNames[i]).size() > targetPartitionNo) {
        int lastElementIndex = masterAssignmentMap.get(instanceNames[i]).size() - 1;
        orphanPartitions.add(masterAssignmentMap.get(instanceNames[i]).get(lastElementIndex));
        masterAssignmentMap.get(instanceNames[i]).remove(lastElementIndex);
      }
    }
    leave = totalPartitions % masterAssignmentMap.size();
    Collections.sort(orphanPartitions);
    // Assign "orphaned" partitions to hosts that do not have enough partitions
    for (int i = 0; i < instanceNames.length; i++) {
      int targetPartitionNo = leave > 0 ? (partitionNumber + 1) : partitionNumber;
      leave--;
      if (targetPartitionNo > maxPartitionsPerInstance) {
        targetPartitionNo = maxPartitionsPerInstance;
      }
      while (masterAssignmentMap.get(instanceNames[i]).size() < targetPartitionNo) {
        int lastElementIndex = orphanPartitions.size() - 1;
        masterAssignmentMap.get(instanceNames[i]).add(orphanPartitions.get(lastElementIndex));
        orphanPartitions.remove(lastElementIndex);
      }
    }
    if (orphanPartitions.size() > 0) {
      LOG.warn("orphanPartitions still contains elements");
    }
  }

  /**
   * Generate full preference list from the master assignment map evenly distribute the
   * slave partitions mastered on a host to other hosts
   * @param masterAssignmentMap
   *          current master assignment map
   * @param orphanPartitions
   *          partitions not hosted by any instance
   * @return
   */
  private Map<String, List<String>> generateListFieldFromMasterAssignment(
      Map<String, List<String>> masterAssignmentMap, int replicas) {
    Map<String, List<String>> listFields = new HashMap<String, List<String>>();
    int slaves = replicas - 1;
    String[] instanceNames = new String[masterAssignmentMap.size()];
    masterAssignmentMap.keySet().toArray(instanceNames);
    Arrays.sort(instanceNames);

    for (int i = 0; i < instanceNames.length; i++) {
      String instanceName = instanceNames[i];
      List<String> otherInstances = new ArrayList<String>(masterAssignmentMap.size() - 1);
      for (int x = 0; x < instanceNames.length - 1; x++) {
        int index = (x + i + 1) % instanceNames.length;
        otherInstances.add(instanceNames[index]);
      }

      List<String> partitionList = masterAssignmentMap.get(instanceName);
      for (int j = 0; j < partitionList.size(); j++) {
        String partitionName = partitionList.get(j);
        listFields.put(partitionName, new ArrayList<String>());
        listFields.get(partitionName).add(instanceName);

        int slavesCanAssign = Math.min(slaves, otherInstances.size());
        for (int k = 0; k < slavesCanAssign; k++) {
          int index = (j + k + 1) % otherInstances.size();
          listFields.get(partitionName).add(otherInstances.get(index));
        }
      }
    }
    return listFields;
  }
}
