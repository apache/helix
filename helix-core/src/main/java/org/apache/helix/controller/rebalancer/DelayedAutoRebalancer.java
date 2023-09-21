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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import java.util.stream.Collectors;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.constraint.MonitoredAbnormalResolver;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the Full-Auto Rebalancer that is featured with delayed partition movement.
 */
public class DelayedAutoRebalancer extends AbstractRebalancer<ResourceControllerDataProvider> {
  private static final Logger LOG = LoggerFactory.getLogger(DelayedAutoRebalancer.class);
   public static final Set<String> INSTANCE_OPERATION_TO_EXCLUDE_FROM_ASSIGNMENT = ImmutableSet.of("EVACUATE", "SWAP_IN");

  @Override
  public IdealState computeNewIdealState(String resourceName,
      IdealState currentIdealState, CurrentStateOutput currentStateOutput,
      ResourceControllerDataProvider clusterData) {

    IdealState cachedIdealState = getCachedIdealState(resourceName, clusterData);
    if (cachedIdealState != null) {
      LOG.debug("Use cached IdealState for {}", resourceName);
      return cachedIdealState;
    }

    LOG.info("Computing IdealState for " + resourceName);

    List<String> allPartitions = getStablePartitionList(clusterData, currentIdealState);
    if (allPartitions.size() == 0) {
      LOG.info("Partition count is 0 for resource " + resourceName
          + ", stop calculate ideal mapping for the resource.");
      return generateNewIdealState(resourceName, currentIdealState,
          emptyMapping(currentIdealState));
    }

    Map<String, List<String>> userDefinedPreferenceList = new HashMap<>();

    ClusterConfig clusterConfig = clusterData.getClusterConfig();
    ResourceConfig resourceConfig = clusterData.getResourceConfig(resourceName);
    boolean delayRebalanceEnabled =
        DelayedRebalanceUtil.isDelayRebalanceEnabled(currentIdealState, clusterConfig);

    if (resourceConfig != null) {
      userDefinedPreferenceList = resourceConfig.getPreferenceLists();
      if (!userDefinedPreferenceList.isEmpty()) {
        LOG.info("Using user defined preference list for partitions: " + userDefinedPreferenceList
            .keySet());
      }
    }

    Set<String> liveEnabledNodes;
    Set<String> allNodes;

    String instanceTag = currentIdealState.getInstanceGroupTag();
    if (instanceTag != null) {
      liveEnabledNodes = clusterData.getEnabledLiveInstancesWithTag(instanceTag);
      allNodes = clusterData.getInstancesWithTag(instanceTag);

      if (LOG.isInfoEnabled()) {
        LOG.info(String.format(
            "Found the following participants with tag %s for %s: "
                + "instances: %s, liveEnabledInstances: %s",
            currentIdealState.getInstanceGroupTag(), resourceName, allNodes, liveEnabledNodes));
      }
    } else {
      liveEnabledNodes = clusterData.getEnabledLiveInstances();
      allNodes = clusterData.getAllInstances();
    }

    long delay = DelayedRebalanceUtil.getRebalanceDelay(currentIdealState, clusterConfig);
    Set<String> activeNodes = DelayedRebalanceUtil
        .getActiveNodes(allNodes, currentIdealState, liveEnabledNodes,
            clusterData.getInstanceOfflineTimeMap(), clusterData.getLiveInstances().keySet(),
            clusterData.getInstanceConfigMap(), delay, clusterConfig);
    if (delayRebalanceEnabled) {
      Set<String> offlineOrDisabledInstances = new HashSet<>(activeNodes);
      offlineOrDisabledInstances.removeAll(liveEnabledNodes);
      DelayedRebalanceUtil.setRebalanceScheduler(currentIdealState.getResourceName(), true,
          offlineOrDisabledInstances, clusterData.getInstanceOfflineTimeMap(),
          clusterData.getLiveInstances().keySet(), clusterData.getInstanceConfigMap(), delay,
          clusterConfig, _manager);
    }

    if (allNodes.isEmpty() || activeNodes.isEmpty()) {
      LOG.error(String.format(
          "No instances or active instances available for resource %s, "
              + "allInstances: %s, liveInstances: %s, activeInstances: %s",
          resourceName, allNodes, liveEnabledNodes, activeNodes));
      return generateNewIdealState(resourceName, currentIdealState,
          emptyMapping(currentIdealState));
    }

    StateModelDefinition stateModelDef =
        clusterData.getStateModelDef(currentIdealState.getStateModelDefRef());

    int replicaCount = currentIdealState.getReplicaCount(activeNodes.size());
    if (replicaCount == 0) {
      LOG.error("Replica count is 0 for resource " + resourceName
          + ", stop calculate ideal mapping for the resource.");
      return generateNewIdealState(resourceName, currentIdealState,
          emptyMapping(currentIdealState));
    }

    LinkedHashMap<String, Integer> stateCountMap =
        stateModelDef.getStateCountMap(activeNodes.size(), replicaCount);
    Map<String, Map<String, String>> currentMapping =
        currentMapping(currentStateOutput, resourceName, allPartitions, stateCountMap);

    int maxPartition = currentIdealState.getMaxPartitionsPerInstance();
    _rebalanceStrategy =
        getRebalanceStrategy(currentIdealState.getRebalanceStrategy(), allPartitions, resourceName,
            stateCountMap, maxPartition);

    // sort node lists to ensure consistent preferred assignments
    List<String> allNodeList = new ArrayList<>(allNodes);
    // We will not assign partition to instances with evacuation and wap-out tag.
    // TODO: Currently we have 2 groups of instances and compute preference list twice and merge.
    // Eventually we want to have exclusive groups of instance for different instance tag.
    List<String> liveEnabledAssignableNodeList = filterOutOnOperationInstances(clusterData.getInstanceConfigMap(),
        liveEnabledNodes);
    Collections.sort(allNodeList);
    Collections.sort(liveEnabledAssignableNodeList);

    ZNRecord newIdealMapping = _rebalanceStrategy
        .computePartitionAssignment(allNodeList, liveEnabledAssignableNodeList, currentMapping, clusterData);
    ZNRecord finalMapping = newIdealMapping;

    if (DelayedRebalanceUtil.isDelayRebalanceEnabled(currentIdealState, clusterConfig)
        || liveEnabledAssignableNodeList.size()!= activeNodes.size()) {
      List<String> activeNodeList = new ArrayList<>(activeNodes);
      Collections.sort(activeNodeList);
      int minActiveReplicas = DelayedRebalanceUtil.getMinActiveReplica(
          ResourceConfig.mergeIdealStateWithResourceConfig(resourceConfig, currentIdealState),
          currentIdealState, replicaCount);

      ZNRecord newActiveMapping = _rebalanceStrategy
          .computePartitionAssignment(allNodeList, activeNodeList, currentMapping, clusterData);
      finalMapping = getFinalDelayedMapping(currentIdealState, newIdealMapping, newActiveMapping,
          liveEnabledNodes, replicaCount, minActiveReplicas);
    }

    finalMapping.getListFields().putAll(userDefinedPreferenceList);

    LOG.debug("currentMapping: {}", currentMapping);
    LOG.debug("stateCountMap: {}", stateCountMap);
    LOG.debug("liveEnabledNodes: {}", liveEnabledNodes);
    LOG.debug("activeNodes: {}", activeNodes);
    LOG.debug("allNodes: {}", allNodes);
    LOG.debug("maxPartition: {}", maxPartition);
    LOG.debug("newIdealMapping: {}", newIdealMapping);
    LOG.debug("finalMapping: {}", finalMapping);

    IdealState idealState = generateNewIdealState(resourceName, currentIdealState, finalMapping);
    clusterData.setCachedIdealMapping(resourceName, idealState.getRecord());
    return idealState;
  }

  private static List<String> filterOutOnOperationInstances(Map<String, InstanceConfig> instanceConfigMap,
      Set<String> nodes) {
    return nodes.stream()
        .filter(
            instance -> !INSTANCE_OPERATION_TO_EXCLUDE_FROM_ASSIGNMENT.contains(instanceConfigMap.get(instance).getInstanceOperation()))
        .collect(Collectors.toList());
  }

  private IdealState generateNewIdealState(String resourceName, IdealState currentIdealState,
      ZNRecord newMapping) {
    IdealState newIdealState = new IdealState(resourceName);
    newIdealState.getRecord().setSimpleFields(currentIdealState.getRecord().getSimpleFields());
    newIdealState.setRebalanceMode(currentIdealState.getRebalanceMode());
    newIdealState.getRecord().setListFields(newMapping.getListFields());

    return newIdealState;
  }

  private ZNRecord getFinalDelayedMapping(IdealState idealState, ZNRecord newIdealMapping,
      ZNRecord newActiveMapping, Set<String> liveInstances, int numReplica, int minActiveReplica) {
    if (minActiveReplica >= numReplica) {
      return newIdealMapping;
    }
    ZNRecord finalMapping = new ZNRecord(idealState.getResourceName());
    finalMapping.setListFields(DelayedRebalanceUtil
        .getFinalDelayedMapping(newIdealMapping.getListFields(), newActiveMapping.getListFields(),
            liveInstances, minActiveReplica));
    return finalMapping;
  }

  private ZNRecord emptyMapping(IdealState idealState) {
    ZNRecord emptyMapping = new ZNRecord(idealState.getResourceName());
    for (String partition : idealState.getPartitionSet()) {
      emptyMapping.setListField(partition, new ArrayList<String>());
    }
    return emptyMapping;
  }

  /**
   * Compute the best state for all partitions.
   * This is the default implementation, subclasses should re-implement
   * this method if its logic to generate bestpossible map for each partition is different from the default one here.
   *
   * @param cache
   * @param idealState
   * @param resource
   * @param currentStateOutput Provides the current state and pending state transitions for all partitions
   * @return
   */
  @Override
  public ResourceAssignment computeBestPossiblePartitionState(ResourceControllerDataProvider cache,
      IdealState idealState, Resource resource, CurrentStateOutput currentStateOutput) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing resource:" + resource.getResourceName());
    }

    Set<String> allNodes = cache.getEnabledInstances();
    Set<String> liveNodes = cache.getLiveInstances().keySet();

    ClusterConfig clusterConfig = cache.getClusterConfig();
    long delayTime = DelayedRebalanceUtil.getRebalanceDelay(idealState, clusterConfig);
    Set<String> activeNodes = DelayedRebalanceUtil
        .getActiveNodes(allNodes, idealState, liveNodes, cache.getInstanceOfflineTimeMap(),
            cache.getLiveInstances().keySet(), cache.getInstanceConfigMap(), delayTime,
            clusterConfig);

    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    ResourceAssignment partitionMapping = new ResourceAssignment(resource.getResourceName());

    for (Partition partition : resource.getPartitions()) {
      Set<String> disabledInstancesForPartition =
          cache.getDisabledInstancesForPartition(resource.getResourceName(), partition.toString());
      List<String> preferenceList = getPreferenceList(partition, idealState, activeNodes);
      Map<String, String> bestStateForPartition =
          computeBestPossibleStateForPartition(liveNodes, stateModelDef, preferenceList,
              currentStateOutput, disabledInstancesForPartition, idealState, clusterConfig,
              partition, cache.getAbnormalStateResolver(stateModelDefName), cache);

      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Best possible mapping for resource  " + resource.getResourceName() + ": "
          + partitionMapping);
    }

    return partitionMapping;
  }


  @Override
  protected Map<String, String> computeBestPossibleStateForPartition(Set<String> liveInstances,
      StateModelDefinition stateModelDef, List<String> preferenceList,
      CurrentStateOutput currentStateOutput, Set<String> disabledInstancesForPartition,
      IdealState idealState, ClusterConfig clusterConfig, Partition partition,
      MonitoredAbnormalResolver monitoredResolver) {

    return computeBestPossibleStateForPartition(liveInstances, stateModelDef, preferenceList,
        currentStateOutput, disabledInstancesForPartition, idealState, clusterConfig, partition,
        monitoredResolver, null);
  }

  @Override
  protected Map<String, String> computeBestPossibleStateForPartition(Set<String> liveInstances,
      StateModelDefinition stateModelDef, List<String> preferenceList,
      CurrentStateOutput currentStateOutput, Set<String> disabledInstancesForPartition,
      IdealState idealState, ClusterConfig clusterConfig, Partition partition,
      MonitoredAbnormalResolver monitoredResolver, ResourceControllerDataProvider cache) {

    Optional<Map<String, String>> optionalOverwrittenStates =
        computeStatesOverwriteForPartition(stateModelDef, preferenceList, currentStateOutput,
            idealState, partition, monitoredResolver);
    if (optionalOverwrittenStates.isPresent()) {
      return optionalOverwrittenStates.get();
    }
    Map<String, String> currentStateMap = new HashMap<>(
        currentStateOutput.getCurrentStateMap(idealState.getResourceName(), partition));
    // Instances not in preference list but still have active replica, retain to avoid zero replica during movement
    List<String> currentInstances = new ArrayList<>(currentStateMap.keySet());
    Collections.sort(currentInstances);
    Map<String, String> pendingStates =
        new HashMap<>(currentStateOutput.getPendingStateMap(idealState.getResourceName(), partition));
    for (String instance : pendingStates.keySet()) {
      if (!currentStateMap.containsKey(instance)) {
        currentStateMap.put(instance, stateModelDef.getInitialState());
        currentInstances.add(instance);
      }
    }

    Set<String> instancesToDrop = new HashSet<>();
    Iterator<String> it = currentInstances.iterator();
    while (it.hasNext()) {
      String instance = it.next();
      String state = currentStateMap.get(instance);
      if (state == null) {
        it.remove();
        instancesToDrop.add(instance); // These instances should be set to DROPPED after we get bestPossibleStateMap;
      }
    }

    // Sort the instancesToMove by their current partition state.
    // Reason: because the states are assigned to instances in the order appeared in preferenceList, if we have
    // [node1:Slave, node2:Master], we want to keep it that way, instead of assigning Master to node1.

    if (preferenceList == null) {
      preferenceList = Collections.emptyList();
    }
    boolean isPreferenceListEmpty = preferenceList.isEmpty();
    int numExtraReplicas = getNumExtraReplicas(clusterConfig);

    // TODO : Keep the behavior consistent with existing state count, change back to read from idealstate
    // replicas
    int numReplicas = preferenceList.size();
    List<String> instanceToAdd = new ArrayList<>(preferenceList);
    instanceToAdd.removeAll(currentInstances);
    List<String> combinedPreferenceList = new ArrayList<>();

    if (currentInstances.size() <= numReplicas
        && numReplicas + numExtraReplicas - currentInstances.size() > 0) {
      int subListSize = numReplicas + numExtraReplicas - currentInstances.size();
      combinedPreferenceList.addAll(instanceToAdd
          .subList(0, Math.min(subListSize, instanceToAdd.size())));
    }

    // Make all initial state instance not in preference list to be dropped.
    Map<String, String> currentMapWithPreferenceList = new HashMap<>(currentStateMap);
    currentMapWithPreferenceList.keySet().retainAll(preferenceList);

    combinedPreferenceList.addAll(currentInstances);
    combinedPreferenceList.sort(new PreferenceListNodeComparator(currentStateMap, stateModelDef, preferenceList));

    // if preference list is not empty, and we do have new intanceToAdd, we
    // should check if it has capacity to hold the partition.
    boolean isWaged = WagedValidationUtil.isWagedEnabled(idealState) && cache != null;
    if (isWaged && !isPreferenceListEmpty && instanceToAdd.size() > 0) {
      // check instanceToAdd instance appears in combinedPreferenceList
      for (String instance : instanceToAdd) {
        if (combinedPreferenceList.contains(instance)) {
          if (!cache.checkAndReduceCapacity(instance, idealState.getResourceName(),
              partition.getPartitionName())) {
            // if instanceToAdd instance has no capacity to hold the partition, we should
            // remove it from combinedPreferenceList
            LOG.info("Instance: {} has no capacity to hold resource: {}, partition: {}, removing "
                + "it from combinedPreferenceList.", instance, idealState.getResourceName(),
                partition.getPartitionName());
            combinedPreferenceList.remove(instance);
          }
        }
      }
    }

    // Assign states to instances with the combined preference list.
    Map<String, String> bestPossibleStateMap =
        computeBestPossibleMap(combinedPreferenceList, stateModelDef, currentStateMap,
            liveInstances, disabledInstancesForPartition);

    for (String instance : instancesToDrop) {
      bestPossibleStateMap.put(instance, HelixDefinedState.DROPPED.name());
    }

    // If the load-balance finishes (all replica are migrated to new instances),
    // we should drop all partitions from previous assigned instances.
    if (!currentMapWithPreferenceList.values().contains(HelixDefinedState.ERROR.name())
        && bestPossibleStateMap.size() > numReplicas && readyToDrop(currentStateMap,
        bestPossibleStateMap, preferenceList, combinedPreferenceList)) {
      for (int i = 0; i < combinedPreferenceList.size() - numReplicas; i++) {
        String instanceToDrop = combinedPreferenceList.get(combinedPreferenceList.size() - i - 1);
        bestPossibleStateMap.put(instanceToDrop, HelixDefinedState.DROPPED.name());
      }
    }

    // Adding ERROR replica mapping to best possible
    // ERROR assignment should be mutual excluded from DROPPED assignment because
    // once there is an ERROR replica in the mapping, bestPossibleStateMap.size() > numReplicas prevents
    // code entering the DROPPING stage.
    for (String instance : combinedPreferenceList) {
      if (currentStateMap.containsKey(instance) && currentStateMap.get(instance)
          .equals(HelixDefinedState.ERROR.name())) {
        bestPossibleStateMap.put(instance, HelixDefinedState.ERROR.name());
      }
    }
    return bestPossibleStateMap;
  }

  private boolean readyToDrop(Map<String, String> currentStateMap,
      Map<String, String> bestPossibleMap, List<String> preferenceList,
      List<String> combinedPreferenceList) {
    Set<String> preferenceWithActiveState = new HashSet<>(preferenceList);
    preferenceWithActiveState.retainAll(combinedPreferenceList);

    for (String instance : preferenceWithActiveState) {
      if (!currentStateMap.containsKey(instance) || !currentStateMap.get(instance)
          .equals(bestPossibleMap.get(instance))) {
        return false;
      }
    }
    return true;
  }

  private int getNumExtraReplicas(ClusterConfig clusterConfig) {
    int numExtraReplicas = StateTransitionThrottleConfig.DEFAULT_NUM_TRANSIT_REPLICAS;
    List<StateTransitionThrottleConfig> stateTransitionThrottleConfigs =
        clusterConfig.getStateTransitionThrottleConfigs();

    for (StateTransitionThrottleConfig throttleConfig : stateTransitionThrottleConfigs) {
      if (StateTransitionThrottleConfig.ThrottleScope.PARTITION
          .equals(throttleConfig.getThrottleScope())
          && StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE
          .equals(throttleConfig.getRebalanceType())) {
        numExtraReplicas =
            (int) Math.min(numExtraReplicas, throttleConfig.getMaxPartitionInTransition());
      }
    }
    return numExtraReplicas;
  }
}
