package org.apache.helix.controller.rebalancer.util;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixConstants.StateModelToken;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

/**
 * Collection of functions that will compute the best possible states given the live instances and
 * an ideal state.
 */
public class ConstraintBasedAssignment {
  private static Logger logger = Logger.getLogger(ConstraintBasedAssignment.class);

  public static List<String> getPreferenceList(ClusterDataCache cache, Partition resource,
      IdealState idealState, StateModelDefinition stateModelDef) {
    List<String> listField = idealState.getPreferenceList(resource.getPartitionName());

    if (listField != null && listField.size() == 1
        && StateModelToken.ANY_LIVEINSTANCE.toString().equals(listField.get(0))) {
      Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
      List<String> prefList = new ArrayList<String>(liveInstances.keySet());
      Collections.sort(prefList);
      return prefList;
    } else {
      return listField;
    }
  }

  /**
   * compute best state for resource in AUTO ideal state mode
   * @param cache
   * @param stateModelDef
   * @param instancePreferenceList
   * @param currentStateMap
   *          : instance->state for each partition
   * @param disabledInstancesForPartition
   * @return
   */
  public static Map<String, String> computeAutoBestStateForPartition(ClusterDataCache cache,
      StateModelDefinition stateModelDef, List<String> instancePreferenceList,
      Map<String, String> currentStateMap, Set<String> disabledInstancesForPartition) {
    Map<String, String> instanceStateMap = new HashMap<String, String>();

    // if the ideal state is deleted, instancePreferenceList will be empty and
    // we should drop all resources.
    if (currentStateMap != null) {
      for (String instance : currentStateMap.keySet()) {
        if ((instancePreferenceList == null || !instancePreferenceList.contains(instance))
            && !disabledInstancesForPartition.contains(instance)) {
          // if dropped and not disabled, transit to DROPPED
          instanceStateMap.put(instance, HelixDefinedState.DROPPED.toString());
        } else if ((currentStateMap.get(instance) == null || !currentStateMap.get(instance).equals(
            HelixDefinedState.ERROR.toString()))
            && disabledInstancesForPartition.contains(instance)) {
          // if disabled and not in ERROR state, transit to initial-state (e.g. OFFLINE)
          instanceStateMap.put(instance, stateModelDef.getInitialState());
        }
      }
    }

    // ideal state is deleted
    if (instancePreferenceList == null) {
      return instanceStateMap;
    }

    List<String> statesPriorityList = stateModelDef.getStatesPriorityList();
    boolean assigned[] = new boolean[instancePreferenceList.size()];

    Map<String, LiveInstance> liveInstancesMap = cache.getLiveInstances();

    for (String state : statesPriorityList) {
      String num = stateModelDef.getNumInstancesPerState(state);
      int stateCount = -1;
      if ("N".equals(num)) {
        Set<String> liveAndEnabled = new HashSet<String>(liveInstancesMap.keySet());
        liveAndEnabled.removeAll(disabledInstancesForPartition);
        stateCount = liveAndEnabled.size();
      } else if ("R".equals(num)) {
        stateCount = instancePreferenceList.size();
      } else {
        try {
          stateCount = Integer.parseInt(num);
        } catch (Exception e) {
          logger.error("Invalid count for state:" + state + " ,count=" + num);
        }
      }
      if (stateCount > -1) {
        int count = 0;
        for (int i = 0; i < instancePreferenceList.size(); i++) {
          String instanceName = instancePreferenceList.get(i);

          boolean notInErrorState =
              currentStateMap == null || currentStateMap.get(instanceName) == null
                  || !currentStateMap.get(instanceName).equals(HelixDefinedState.ERROR.toString());

          if (liveInstancesMap.containsKey(instanceName) && !assigned[i] && notInErrorState
              && !disabledInstancesForPartition.contains(instanceName)) {
            instanceStateMap.put(instanceName, state);
            count = count + 1;
            assigned[i] = true;
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
