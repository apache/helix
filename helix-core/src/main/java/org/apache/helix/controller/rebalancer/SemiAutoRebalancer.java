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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

/**
 * This is a Rebalancer specific to semi-automatic mode. It is tasked with computing the ideal
 * state of a resource based on a predefined preference list of instances willing to accept
 * replicas.
 * The input is the optional current assignment of partitions to instances, as well as the required
 * existing instance preferences.
 * The output is a mapping based on that preference list, i.e. partition p has a replica on node k
 * with state s.
 */
public class SemiAutoRebalancer extends AbstractRebalancer {
  private static final Logger LOG = Logger.getLogger(SemiAutoRebalancer.class);

  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
    return currentIdealState;
  }

  @Override
  public Map<String, String> computeAutoBestStateForPartition(ClusterDataCache cache,
      StateModelDefinition stateModelDef, List<String> instancePreferenceList,
      Map<String, String> currentStateMap, Set<String> disabledInstancesForPartition,
      boolean isResourceEnabled) {
    Map<String, String> instanceStateMap = new HashMap<String, String>();

    if (currentStateMap != null) {
      for (String instance : currentStateMap.keySet()) {
        if (instancePreferenceList == null || !instancePreferenceList.contains(instance)) {
          // The partition is dropped from preference list.
          // Transit to DROPPED no matter the instance is disabled or not.
          instanceStateMap.put(instance, HelixDefinedState.DROPPED.toString());
        } else {
          // if disabled and not in ERROR state, transit to initial-state (e.g. OFFLINE)
          if (disabledInstancesForPartition.contains(instance) || !isResourceEnabled) {
            if (currentStateMap.get(instance) == null || !currentStateMap.get(instance)
                .equals(HelixDefinedState.ERROR.name())) {
              instanceStateMap.put(instance, stateModelDef.getInitialState());
            }
          }
        }
      }
    }

    // if the ideal state is deleted, instancePreferenceList will be empty and
    // we should drop all resources.
    if (instancePreferenceList == null) {
      return instanceStateMap;
    }

    List<String> statesPriorityList = stateModelDef.getStatesPriorityList();
    boolean assigned[] = new boolean[instancePreferenceList.size()];

    Map<String, LiveInstance> liveInstancesMap = cache.getLiveInstances();
    Set<String> secondTopStates = stateModelDef.getSecondTopStates();
    String topState = statesPriorityList.get(0);
    int occupiedTopState = 0;
    for (String currentState : currentStateMap.values()) {
      if (currentState.equals(topState)) {
        occupiedTopState++;
      }
    }

    for (String state : statesPriorityList) {
      String num = stateModelDef.getNumInstancesPerState(state);
      int stateCount = -1;
      if ("N".equals(num)) {
        Set<String> liveAndEnabled = new HashSet<String>(liveInstancesMap.keySet());
        liveAndEnabled.removeAll(disabledInstancesForPartition);
        stateCount = isResourceEnabled ? liveAndEnabled.size() : 0;
      } else if ("R".equals(num)) {
        stateCount = instancePreferenceList.size();
      } else {
        try {
          stateCount = Integer.parseInt(num);
        } catch (Exception e) {
          LOG.error("Invalid count for state:" + state + " ,count=" + num);
        }
      }
      if (stateCount > -1) {
        int count = 0;
        for (int i = 0; i < instancePreferenceList.size(); i++) {
          String instanceName = instancePreferenceList.get(i);

          boolean notInErrorState =
              currentStateMap == null || currentStateMap.get(instanceName) == null
                  || !currentStateMap.get(instanceName).equals(HelixDefinedState.ERROR.toString());

          boolean enabled =
              !disabledInstancesForPartition.contains(instanceName) && isResourceEnabled;

          String currentState =
              (currentStateMap == null || currentStateMap.get(instanceName) == null)
                  ? stateModelDef.getInitialState() : currentStateMap.get(instanceName);
          if (liveInstancesMap.containsKey(instanceName) && !assigned[i] && notInErrorState
              && enabled) {
            // If target state is top state : 1. Still have extra top state count not assigned
            //                                2. Current state is is at second top state
            //                                3. Current state is at top state
            if (state.equals(topState) && stateCount - occupiedTopState <= 0 && !currentState
                .equals(topState) && !secondTopStates.contains(currentState)) {
              continue;
            }
            instanceStateMap.put(instanceName, state);
            count = count + 1;
            assigned[i] = true;
            occupiedTopState++;
            if (count == stateCount) {
              break;
            }
          }
        }
      }
    }
    return instanceStateMap;
  }
}
