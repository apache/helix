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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a Rebalancer specific to custom mode. It is tasked with checking an existing mapping of
 * partitions against the set of live instances to mark assignment states as dropped or erroneous
 * as necessary.
 * The input is the required current assignment of partitions to instances, as well as the required
 * existing instance preferences.
 * The output is a verified mapping based on that preference list, i.e. partition p has a replica
 * on node k with state s, where s may be a dropped or error state if necessary.
 */
public class CustomRebalancer extends AbstractRebalancer {
  private static final Logger LOG = LoggerFactory.getLogger(CustomRebalancer.class);

  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
    return currentIdealState;
  }

  @Override
  public ResourceAssignment computeBestPossiblePartitionState(ClusterDataCache cache,
      IdealState idealState, Resource resource, CurrentStateOutput currentStateOutput) {
    // Looking for cached BestPossible mapping for this resource, if it is already there, do not recompute it again.
    // The cached mapping will be cleared in ClusterDataCache if there is anything changed in cluster state that can
    // cause the potential changes in BestPossible state.
    ResourceAssignment partitionMapping =
        cache.getCachedResourceAssignment(resource.getResourceName());
    if (partitionMapping != null) {
      return partitionMapping;
    }

    LOG.info("Computing BestPossibleMapping for " + resource);

    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing resource:" + resource.getResourceName());
    }
    partitionMapping = new ResourceAssignment(resource.getResourceName());
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resource.getResourceName(), partition);
      Set<String> disabledInstancesForPartition =
          cache.getDisabledInstancesForPartition(resource.getResourceName(), partition.toString());
      Map<String, String> idealStateMap =
          idealState.getInstanceStateMap(partition.getPartitionName());
      Map<String, String> bestStateForPartition =
          computeCustomizedBestStateForPartition(cache, stateModelDef, idealStateMap,
              currentStateMap, disabledInstancesForPartition, idealState.isEnabled());
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }

    cache.setCachedResourceAssignment(resource.getResourceName(), partitionMapping);

    return partitionMapping;
  }

  /**
   * compute best state for resource in CUSTOMIZED ideal state mode
   * @param cache
   * @param stateModelDef
   * @param idealStateMap
   * @param currentStateMap
   * @param disabledInstancesForPartition
   * @param isResourceEnabled
   * @return
   */
  private Map<String, String> computeCustomizedBestStateForPartition(ClusterDataCache cache,
      StateModelDefinition stateModelDef, Map<String, String> idealStateMap,
      Map<String, String> currentStateMap, Set<String> disabledInstancesForPartition,
      boolean isResourceEnabled) {
    Map<String, String> instanceStateMap = new HashMap<String, String>();

    // if the ideal state is deleted, idealStateMap will be null/empty and
    // we should drop all resources.
    if (currentStateMap != null) {
      for (String instance : currentStateMap.keySet()) {
        if ((idealStateMap == null || !idealStateMap.containsKey(instance))
            && !disabledInstancesForPartition.contains(instance)) {
          // if dropped (whether disabled or not), transit to DROPPED
          instanceStateMap.put(instance, HelixDefinedState.DROPPED.toString());
        } else if ((currentStateMap.get(instance) == null || !currentStateMap.get(instance).equals(
            HelixDefinedState.ERROR.name()))
            && (!isResourceEnabled || disabledInstancesForPartition.contains(instance))) {
          // if disabled and not in ERROR state, transit to initial-state (e.g. OFFLINE)
          instanceStateMap.put(instance, stateModelDef.getInitialState());
        }
      }
    }

    // ideal state is deleted
    if (idealStateMap == null) {
      return instanceStateMap;
    }

    Map<String, LiveInstance> liveInstancesMap = cache.getLiveInstances();
    for (String instance : idealStateMap.keySet()) {
      boolean notInErrorState =
          currentStateMap == null || currentStateMap.get(instance) == null
              || !currentStateMap.get(instance).equals(HelixDefinedState.ERROR.toString());

      boolean enabled = !disabledInstancesForPartition.contains(instance) && isResourceEnabled;

      if (liveInstancesMap.containsKey(instance) && notInErrorState && enabled) {
        instanceStateMap.put(instance, idealStateMap.get(instance));
      }
    }

    return instanceStateMap;
  }
}
