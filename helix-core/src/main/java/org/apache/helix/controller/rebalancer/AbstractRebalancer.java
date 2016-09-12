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

import org.apache.helix.HelixManager;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is a abstract rebalancer that defines some default behaviors for Helix rebalancer
 * as well as all utility functions that will be used by all specific rebalancers.
 */
public abstract class AbstractRebalancer implements Rebalancer, MappingCalculator {
  // These should be final, but are initialized in init rather than a constructor
  protected HelixManager _manager;
  protected RebalanceStrategy _rebalanceStrategy;

  private static final Logger LOG = Logger.getLogger(AbstractRebalancer.class);

  @Override
  public void init(HelixManager manager) {
    this._manager = manager;
    this._rebalanceStrategy = null;
  }

  @Override
  public abstract IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ClusterDataCache clusterData);

  /**
   * Compute the best state for all partitions.
   * This is the default ConstraintBasedAssignment implementation, subclasses should re-implement
   * this method if its logic to generate bestpossible map for each partition is different from the default one here.
   *
   * @param cache
   * @param idealState
   * @param resource
   * @param currentStateOutput
   *          Provides the current state and pending state transitions for all partitions
   * @return
   */
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

  /**
   * @return state count map: state->count
   */
  protected static LinkedHashMap<String, Integer> stateCount(StateModelDefinition stateModelDef,
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

  protected Map<String, Map<String, String>> currentMapping(CurrentStateOutput currentStateOutput,
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
}
