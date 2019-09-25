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
import org.apache.helix.HelixManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The util for supporting delayed rebalance logic.
 */
public class DelayedRebalanceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(DelayedRebalanceUtil.class);

  private static RebalanceScheduler _rebalanceScheduler = new RebalanceScheduler();

  /**
   * @return true if delay rebalance is configured and enabled in the ClusterConfig configurations.
   */
  public static boolean isDelayRebalanceEnabled(ClusterConfig clusterConfig) {
    long delay = clusterConfig.getRebalanceDelayTime();
    return (delay > 0 && clusterConfig.isDelayRebalaceEnabled());
  }

  /**
   * @return true if delay rebalance is configured and enabled in Resource IdealState and the
   * ClusterConfig configurations.
   */
  public static boolean isDelayRebalanceEnabled(IdealState idealState,
      ClusterConfig clusterConfig) {
    long delay = getRebalanceDelay(idealState, clusterConfig);
    return (delay > 0 && idealState.isDelayRebalanceEnabled() && clusterConfig
        .isDelayRebalaceEnabled());
  }

  /**
   * @return the rebalance delay based on Resource IdealState and the ClusterConfig configurations.
   */
  public static long getRebalanceDelay(IdealState idealState, ClusterConfig clusterConfig) {
    long delayTime = idealState.getRebalanceDelay();
    if (delayTime < 0) {
      delayTime = clusterConfig.getRebalanceDelayTime();
    }
    return delayTime;
  }

  /**
   * @return all active instances (live instances plus offline-yet-active instances) while
   * considering cluster delay rebalance configurations.
   */
  public static Set<String> getActiveInstances(Set<String> allNodes, Set<String> liveEnabledNodes,
      Map<String, Long> instanceOfflineTimeMap, Set<String> liveNodes,
      Map<String, InstanceConfig> instanceConfigMap, ClusterConfig clusterConfig) {
    if (!isDelayRebalanceEnabled(clusterConfig)) {
      return Collections.emptySet();
    }
    return getActiveInstances(allNodes, liveEnabledNodes, instanceOfflineTimeMap, liveNodes,
        instanceConfigMap, clusterConfig.getRebalanceDelayTime(), clusterConfig);
  }

  /**
   * @return all active instances (live instances plus offline-yet-active instances) while
   * considering cluster and the resource delay rebalance configurations.
   */
  public static Set<String> getActiveInstances(Set<String> allNodes, IdealState idealState,
      Set<String> liveEnabledNodes, Map<String, Long> instanceOfflineTimeMap, Set<String> liveNodes,
      Map<String, InstanceConfig> instanceConfigMap, long delay, ClusterConfig clusterConfig) {
    if (!isDelayRebalanceEnabled(idealState, clusterConfig)) {
      return Collections.emptySet();
    }
    return getActiveInstances(allNodes, liveEnabledNodes, instanceOfflineTimeMap, liveNodes,
        instanceConfigMap, delay, clusterConfig);
  }

  private static Set<String> getActiveInstances(Set<String> allNodes, Set<String> liveEnabledNodes,
      Map<String, Long> instanceOfflineTimeMap, Set<String> liveNodes,
      Map<String, InstanceConfig> instanceConfigMap, long delay, ClusterConfig clusterConfig) {
    Set<String> activeInstances = new HashSet<>(liveEnabledNodes);
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
  private static long getInactiveTime(String instance, Set<String> liveInstances, Long offlineTime,
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
   * Merge the new ideal preference list with the delayed mapping that is calculated based on the
   * delayed rebalance configurations.
   * The method will prioritize the "active" preference list so as to avoid unnecessary transient
   * state transitions.
   *
   * @param newIdealPreferenceList  the ideal mapping that was calculated based on the current
   *                                instance status
   * @param newDelayedPreferenceList the delayed mapping that was calculated based on the delayed
   *                                 instance status
   * @param liveEnabledInstances    list of all the nodes that are both alive and enabled.
   * @param minActiveReplica        the minimum replica count to ensure a valid mapping.
   *                                If the active list does not have enough replica assignment,
   *                                this method will fill the list with the new ideal mapping until
   *                                the replica count satisfies the minimum requirement.
   * @return the merged state mapping.
   */
  public static Map<String, List<String>> getFinalDelayedMapping(
      Map<String, List<String>> newIdealPreferenceList,
      Map<String, List<String>> newDelayedPreferenceList, Set<String> liveEnabledInstances,
      int minActiveReplica) {
    Map<String, List<String>> finalPreferenceList = new HashMap<>();
    for (String partition : newIdealPreferenceList.keySet()) {
      List<String> idealList = newIdealPreferenceList.get(partition);
      List<String> delayedIdealList = newDelayedPreferenceList.get(partition);

      List<String> liveList = new ArrayList<>();
      for (String ins : delayedIdealList) {
        if (liveEnabledInstances.contains(ins)) {
          liveList.add(ins);
        }
      }

      if (liveList.size() >= minActiveReplica) {
        finalPreferenceList.put(partition, delayedIdealList);
      } else {
        List<String> candidates = new ArrayList<>(idealList);
        candidates.removeAll(delayedIdealList);
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
  public static int getMinActiveReplica(IdealState idealState, int replicaCount) {
    int minActiveReplicas = idealState.getMinActiveReplicas();
    if (minActiveReplicas < 0) {
      minActiveReplicas = replicaCount;
    }
    return minActiveReplicas;
  }

  /**
   * Set a rebalance scheduler for the closest future rebalance time.
   */
  public static void setRebalanceScheduler(IdealState idealState,
      Set<String> offlineOrDisabledInstances, Map<String, Long> instanceOfflineTimeMap,
      Set<String> liveNodes, Map<String, InstanceConfig> instanceConfigMap, long delay,
      ClusterConfig clusterConfig, HelixManager manager) {
    String resourceName = idealState.getResourceName();
    if (!isDelayRebalanceEnabled(idealState, clusterConfig)) {
      _rebalanceScheduler.removeScheduledRebalance(resourceName);
      return;
    }

    long currentTime = System.currentTimeMillis();
    long nextRebalanceTime = Long.MAX_VALUE;
    // calculate the closest future rebalance time
    for (String ins : offlineOrDisabledInstances) {
      long inactiveTime = getInactiveTime(ins, liveNodes, instanceOfflineTimeMap.get(ins), delay,
          instanceConfigMap.get(ins), clusterConfig);
      if (inactiveTime != -1 && inactiveTime > currentTime && inactiveTime < nextRebalanceTime) {
        nextRebalanceTime = inactiveTime;
      }
    }

    if (nextRebalanceTime == Long.MAX_VALUE) {
      long startTime = _rebalanceScheduler.removeScheduledRebalance(resourceName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(String
            .format("Remove exist rebalance timer for resource %s at %d\n", resourceName,
                startTime));
      }
    } else {
      long currentScheduledTime = _rebalanceScheduler.getRebalanceTime(resourceName);
      if (currentScheduledTime < 0 || currentScheduledTime > nextRebalanceTime) {
        _rebalanceScheduler.scheduleRebalance(manager, resourceName, nextRebalanceTime);
        if (LOG.isDebugEnabled()) {
          LOG.debug(String
              .format("Set next rebalance time for resource %s at time %d\n", resourceName,
                  nextRebalanceTime));
        }
      }
    }
  }
}
