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

import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The util to support delayed rebalance logic.
 */
public class DelayedRebalanceUtil {
  /**
   * @return the rebalance delay based on the ClusterConfig configurations.
   * Resource level configuration will be ignored.
   */
  public static long getRebalanceDelay(ClusterConfig clusterConfig) {
    return getRebalanceDelay(null, clusterConfig);
  }

  /**
   * @return true if delay rebalance is configured and enabled in the ClusterConfig configurations.
   * Resource level configuration will be ignored.
   */
  public static boolean isDelayRebalanceEnabled(ClusterConfig clusterConfig) {
    return isDelayRebalanceEnabled(null, clusterConfig);
  }

  /**
   * @return all active instances (live instances plus offline-yet-active instances) while considering delay rebalance configurations.
   */
  public static Set<String> getActiveInstances(Set<String> allNodes, Set<String> liveEnabledNodes,
      Map<String, Long> instanceOfflineTimeMap, Set<String> liveNodes,
      Map<String, InstanceConfig> instanceConfigMap, long delay, ClusterConfig clusterConfig) {
    return getActiveInstances(allNodes, null, liveEnabledNodes, instanceOfflineTimeMap, liveNodes,
        instanceConfigMap, delay, clusterConfig);
  }

  /**
   * @return the rebalance delay based on Resource IdealState and the ClusterConfig configurations.
   */
  public static long getRebalanceDelay(IdealState idealState, ClusterConfig clusterConfig) {
    long delayTime = idealState == null ? -1 : idealState.getRebalanceDelay();
    if (delayTime < 0) {
      delayTime = clusterConfig.getRebalanceDelayTime();
    }
    return delayTime;
  }

  /**
   * @return true if delay rebalance is configured and enabled in Resource IdealState or the ClusterConfig configurations.
   */
  public static boolean isDelayRebalanceEnabled(IdealState idealState,
      ClusterConfig clusterConfig) {
    long delay = getRebalanceDelay(idealState, clusterConfig);
    return (delay > 0 && (idealState == null || idealState.isDelayRebalanceEnabled())
        && clusterConfig.isDelayRebalaceEnabled());
  }

  /**
   * @return all active instances (live instances plus offline-yet-active instances) while considering delay rebalance configurations.
   */
  public static Set<String> getActiveInstances(Set<String> allNodes, IdealState idealState,
      Set<String> liveEnabledNodes, Map<String, Long> instanceOfflineTimeMap, Set<String> liveNodes,
      Map<String, InstanceConfig> instanceConfigMap, long delay, ClusterConfig clusterConfig) {
    Set<String> activeInstances = new HashSet<>(liveEnabledNodes);

    if (!isDelayRebalanceEnabled(idealState, clusterConfig)) {
      return activeInstances;
    }

    Set<String> offlineOrDisabledInstances = new HashSet<>(allNodes);
    offlineOrDisabledInstances.removeAll(liveEnabledNodes);

    long currentTime = System.currentTimeMillis();
    for (String ins : offlineOrDisabledInstances) {
      long inactiveTime = getInactiveTime(ins, liveNodes, instanceOfflineTimeMap.get(ins), delay,
          instanceConfigMap.get(ins), clusterConfig);
      InstanceConfig instanceConfig = instanceConfigMap.get(ins);
      if (inactiveTime > currentTime && instanceConfig != null && instanceConfig
          .isDelayRebalanceEnabled()) {
        activeInstances.add(ins);
      }
    }

    return activeInstances;
  }

  /**
   * @return The time when an offline or disabled instance should be treated as inactive.
   * Return -1 if it is inactive now.
   */
  public static long getInactiveTime(String instance, Set<String> liveInstances, Long offlineTime,
      long delay, InstanceConfig instanceConfig, ClusterConfig clusterConfig) {
    long inactiveTime = Long.MAX_VALUE;

    // check the time instance went offline.
    if (!liveInstances.contains(instance)) {
      if (offlineTime != null && offlineTime > 0 && offlineTime + delay < inactiveTime) {
        inactiveTime = offlineTime + delay;
      }
    }

    // check the time instance got disabled.
    if (!instanceConfig.getInstanceEnabled() || (clusterConfig.getDisabledInstances() != null
        && clusterConfig.getDisabledInstances().containsKey(instance))) {
      long disabledTime = instanceConfig.getInstanceEnabledTime();
      if (clusterConfig.getDisabledInstances() != null && clusterConfig.getDisabledInstances()
          .containsKey(instance)) {
        // Update batch disable time
        long batchDisableTime = Long.parseLong(clusterConfig.getDisabledInstances().get(instance));
        if (disabledTime == -1 || disabledTime > batchDisableTime) {
          disabledTime = batchDisableTime;
        }
      }
      if (disabledTime > 0 && disabledTime + delay < inactiveTime) {
        inactiveTime = disabledTime + delay;
      }
    }

    if (inactiveTime == Long.MAX_VALUE) {
      return -1;
    }

    return inactiveTime;
  }

  /**
   * Merge the new ideal preference list with the "active" mapping that is calculated based on the
   * delayed rebalance configuration. The method will prioritize the active preference list so as to
   * avoid unnecessary intermediate change.
   *
   * @param newIdealPreferenceList  the ideal mapping that was calculated based on the current instance status
   * @param newActivePreferenceList the mapping that was calculated based on the delayed instance status
   * @param liveEnabledInstances    list of all the nodes that are both alive and enabled.
   * @param minActiveReplica        the minimum replica count to ensure a valid mapping.
   *                                If the active list does not have enough replica assignment,
   *                                this method will fill the list with the new ideal mapping until
   *                                the replica count satisfies the minimum requirement.
   * @return the merged state mapping.
   */
  public static Map<String, List<String>> getFinalDelayedMapping(
      Map<String, List<String>> newIdealPreferenceList,
      Map<String, List<String>> newActivePreferenceList, Set<String> liveEnabledInstances,
      int minActiveReplica) {
    Map<String, List<String>> finalPreferenceList = new HashMap<>();
    for (String partition : newIdealPreferenceList.keySet()) {
      List<String> idealList = newIdealPreferenceList.get(partition);
      List<String> activeList = newActivePreferenceList.get(partition);

      List<String> liveList = new ArrayList<>();
      int activeReplica = 0;
      for (String ins : activeList) {
        if (liveEnabledInstances.contains(ins)) {
          activeReplica++;
          liveList.add(ins);
        }
      }

      if (activeReplica >= minActiveReplica) {
        finalPreferenceList.put(partition, activeList);
      } else {
        List<String> candidates = new ArrayList<>(idealList);
        candidates.removeAll(activeList);
        for (String liveIns : candidates) {
          liveList.add(liveIns);
          if (liveList.size() >= minActiveReplica) {
            break;
          }
        }
        finalPreferenceList.put(partition, liveList);
      }
    }
    return finalPreferenceList;
  }

  /**
   * Get the minimum active replica count threshold that allows delayed rebalance.
   *
   * @param idealState      the resource Ideal State
   * @param replicaCount the expected active replica count.
   * @return the expected minimum active replica count that is required
   */
  public static  int getMinActiveReplica(IdealState idealState, int replicaCount) {
    int minActiveReplicas = idealState.getMinActiveReplicas();
    if (minActiveReplicas < 0) {
      minActiveReplicas = replicaCount;
    }
    return minActiveReplicas;
  }
}
