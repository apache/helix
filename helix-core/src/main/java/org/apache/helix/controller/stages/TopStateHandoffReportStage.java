package org.apache.helix.controller.stages;

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
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observe top state handoff and report latency
 * TODO: make this stage async
 */
public class TopStateHandoffReportStage extends AbstractBaseStage {
  private static final long DEFAULT_HANDOFF_USER_LATENCY = 0L;
  private static Logger LOG = LoggerFactory.getLogger(TopStateHandoffReportStage.class);
  public static final long TIMESTAMP_NOT_RECORDED = -1L;
  private static final String TASK_STATE_MODEL_NAME = "Task";

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    final ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
    final Long lastPipelineFinishTimestamp = event
        .getAttributeWithDefault(AttributeName.LastRebalanceFinishTimeStamp.name(),
            TIMESTAMP_NOT_RECORDED);
    final Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    final CurrentStateOutput currentStateOutput = event.getAttribute(AttributeName.CURRENT_STATE.name());
    final ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());

    if (cache == null || resourceMap == null || currentStateOutput == null) {
      throw new StageException(
          "Missing critical attributes for stage, requires ClusterDataCache, RESOURCES and CURRENT_STATE");
    }

    if (cache.isTaskCache()) {
      throw new StageException("TopStateHandoffReportStage can only be used in resource pipeline");
    }

    updateTopStateStatus(cache, clusterStatusMonitor, resourceMap, currentStateOutput,
        lastPipelineFinishTimestamp);

  }

  private void updateTopStateStatus(ClusterDataCache cache,
      ClusterStatusMonitor clusterStatusMonitor, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput,
      long lastPipelineFinishTimestamp) {
    Map<String, Map<String, MissingTopStateRecord>> missingTopStateMap =
        cache.getMissingTopStateMap();
    Map<String, Map<String, String>> lastTopStateMap = cache.getLastTopStateLocationMap();

    long durationThreshold = Long.MAX_VALUE;
    if (cache.getClusterConfig() != null) {
      durationThreshold = cache.getClusterConfig().getMissTopStateDurationThreshold();
    }

    // Remove any resource records that no longer exists
    missingTopStateMap.keySet().retainAll(resourceMap.keySet());
    lastTopStateMap.keySet().retainAll(resourceMap.keySet());

    for (Resource resource : resourceMap.values()) {
      StateModelDefinition stateModelDef = cache.getStateModelDef(resource.getStateModelDefRef());
      if (stateModelDef == null || resource.getStateModelDefRef()
          .equalsIgnoreCase(TASK_STATE_MODEL_NAME)) {
        // Resource does not have valid state model, just skip processing
        continue;
      }

      String resourceName = resource.getResourceName();

      for (Partition partition : resource.getPartitions()) {
        String currentTopStateInstance =
            findCurrentTopStateLocation(currentStateOutput, resourceName, partition, stateModelDef);
        String lastTopStateInstance = findCachedTopStateLocation(cache, resourceName, partition);

        if (currentTopStateInstance != null) {
          reportTopStateExistence(cache, currentStateOutput, stateModelDef, resourceName, partition,
              lastTopStateInstance, currentTopStateInstance, clusterStatusMonitor,
              durationThreshold, lastPipelineFinishTimestamp);
          updateCachedTopStateLocation(cache, resourceName, partition, currentTopStateInstance);
        } else {
          reportTopStateMissing(cache, resourceName,
              partition, stateModelDef.getTopState(), currentStateOutput);
          reportTopStateHandoffFailIfNecessary(cache, resourceName, partition, durationThreshold,
              clusterStatusMonitor);
        }
      }
    }

    if (clusterStatusMonitor != null) {
      clusterStatusMonitor.resetMaxMissingTopStateGauge();
    }
  }

  /**
   * From current state output, find out the location of the top state of given resource
   * and partition
   *
   * @param currentStateOutput current state output
   * @param resourceName resource name
   * @param partition partition of the resource
   * @param stateModelDef state model def object
   * @return name of the node that contains top state, null if there is not top state recorded
   */
  private String findCurrentTopStateLocation(CurrentStateOutput currentStateOutput,
      String resourceName, Partition partition, StateModelDefinition stateModelDef) {
    Map<String, String> stateMap = currentStateOutput.getCurrentStateMap(resourceName, partition);
    for (String instance : stateMap.keySet()) {
      if (stateMap.get(instance).equals(stateModelDef.getTopState())) {
        return instance;
      }
    }
    return null;
  }

  /**
   * Find cached top state location of the given resource and partition
   *
   * @param cache cluster data cache object
   * @param resourceName resource name
   * @param partition partition of the given resource
   * @return cached name of the node that contains top state, null if not previously cached
   */
  private String findCachedTopStateLocation(ClusterDataCache cache, String resourceName, Partition partition) {
    Map<String, Map<String, String>> lastTopStateMap = cache.getLastTopStateLocationMap();
    return lastTopStateMap.containsKey(resourceName) && lastTopStateMap.get(resourceName)
        .containsKey(partition.getPartitionName()) ? lastTopStateMap.get(resourceName)
        .get(partition.getPartitionName()) : null;
  }

  /**
   * Update top state location cache of the given resource and partition
   *
   * @param cache cluster data cache object
   * @param resourceName resource name
   * @param partition partition of the given resource
   * @param currentTopStateInstance name of the instance that currently has the top state
   */
  private void updateCachedTopStateLocation(ClusterDataCache cache, String resourceName,
      Partition partition, String currentTopStateInstance) {
    Map<String, Map<String, String>> lastTopStateMap = cache.getLastTopStateLocationMap();
    if (!lastTopStateMap.containsKey(resourceName)) {
      lastTopStateMap.put(resourceName, new HashMap<String, String>());
    }
    lastTopStateMap.get(resourceName).put(partition.getPartitionName(), currentTopStateInstance);
  }

  /**
   * When we observe a top state of a given resource and partition, we need to report for the
   * following 2 scenarios:
   *  1. This is a top state come back, i.e. we have a previously missing top state record
   *  2. Top state location change, i.e. current top state location is different from what
   *     we saw previously
   *
   * @param cache cluster data cache
   * @param currentStateOutput generated after computing current state
   * @param stateModelDef State model definition object of the given resource
   * @param resourceName resource name
   * @param partition partition of the given resource
   * @param lastTopStateInstance our cached top state location
   * @param currentTopStateInstance top state location we observed during this pipeline run
   * @param clusterStatusMonitor monitor object
   * @param durationThreshold top state handoff duration threshold
   * @param lastPipelineFinishTimestamp timestamp when last pipeline run finished
   */
  private void reportTopStateExistence(ClusterDataCache cache, CurrentStateOutput currentStateOutput,
      StateModelDefinition stateModelDef, String resourceName, Partition partition,
      String lastTopStateInstance, String currentTopStateInstance,
      ClusterStatusMonitor clusterStatusMonitor, long durationThreshold,
      long lastPipelineFinishTimestamp) {

    Map<String, Map<String, MissingTopStateRecord>> missingTopStateMap =
        cache.getMissingTopStateMap();

    if (missingTopStateMap.containsKey(resourceName) && missingTopStateMap.get(resourceName)
        .containsKey(partition.getPartitionName())) {
      // We previously recorded a top state missing, and it's not coming back
      reportTopStateComesBack(cache, currentStateOutput.getCurrentStateMap(resourceName, partition),
          resourceName, partition, clusterStatusMonitor, durationThreshold,
          stateModelDef.getTopState());
    } else if (lastTopStateInstance != null && !lastTopStateInstance
        .equals(currentTopStateInstance)) {
      // With no missing top state record, but top state instance changed,
      // we observed an entire top state handoff process
      reportSingleTopStateHandoff(cache, lastTopStateInstance, currentTopStateInstance,
          resourceName, partition, clusterStatusMonitor, lastPipelineFinishTimestamp);
    } else {
      // else, there is not top state change, or top state first came up, do nothing
      LogUtil.logDebug(LOG, _eventId, String.format(
          "No top state hand off or first-seen top state for %s. CurNode: %s, LastNode: %s.",
          partition.getPartitionName(), currentTopStateInstance, lastTopStateInstance));
    }
  }

  /**
   * This function calculates duration of a full top state handoff, observed in 1 pipeline run,
   * i.e. current top state instance loaded from ZK is different than the one we cached during
   * last pipeline run.
   *
   * @param cache ClusterDataCache
   * @param lastTopStateInstance Name of last top state instance we cached
   * @param curTopStateInstance Name of current top state instance we refreshed from ZK
   * @param resourceName resource name
   * @param partition partition object
   * @param clusterStatusMonitor cluster state monitor object
   * @param lastPipelineFinishTimestamp last pipeline run finish timestamp
   */
  private void reportSingleTopStateHandoff(ClusterDataCache cache, String lastTopStateInstance,
      String curTopStateInstance, String resourceName, Partition partition,
      ClusterStatusMonitor clusterStatusMonitor, long lastPipelineFinishTimestamp) {
    if (curTopStateInstance.equals(lastTopStateInstance)) {
      return;
    }

    // Current state output generation logic guarantees that current top state instance
    // must be a live instance
    String curTopStateSession = cache.getLiveInstances().get(curTopStateInstance).getSessionId();
    long endTime =
        cache.getCurrentState(curTopStateInstance, curTopStateSession).get(resourceName)
            .getEndTime(partition.getPartitionName());
    long toTopStateuserLatency =
        endTime - cache.getCurrentState(curTopStateInstance, curTopStateSession).get(resourceName)
            .getStartTime(partition.getPartitionName());

    long startTime = TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED;
    long fromTopStateUserLatency = DEFAULT_HANDOFF_USER_LATENCY;

    // Make sure last top state instance has not bounced during cluster data cache refresh
    if (cache.getLiveInstances().containsKey(lastTopStateInstance)) {
      String lastTopStateSession =
          cache.getLiveInstances().get(lastTopStateInstance).getSessionId();
      // We need this null check as there are test cases creating incomplete current state
      if (cache.getCurrentState(lastTopStateInstance, lastTopStateSession).get(resourceName)
          != null) {
        startTime =
            cache.getCurrentState(lastTopStateInstance, lastTopStateSession).get(resourceName)
                .getStartTime(partition.getPartitionName());
        fromTopStateUserLatency =
            cache.getCurrentState(lastTopStateInstance, lastTopStateSession).get(resourceName)
                .getEndTime(partition.getPartitionName()) - startTime;
      }
    }
    if (startTime == TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED) {
      // either cached last top state instance is no longer alive, or it bounced during cluster
      // data cache refresh, we use last pipeline run end time for best guess. Though we can
      // calculate this number in a more precise way by refreshing data from ZK, given the rarity
      // of this corner case, it's not worthy.
      startTime = lastPipelineFinishTimestamp;
      fromTopStateUserLatency = DEFAULT_HANDOFF_USER_LATENCY;
    }

    if (startTime == TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED || startTime > endTime) {
      // Top state handoff finished before end of last pipeline run, and instance contains
      // previous top state is no longer alive, so our best guess did not work, ignore the
      // data point for now.
      LogUtil.logWarn(LOG, _eventId, String
          .format("Cannot confirm top state missing start time. %s:%s->%s. Likely it was very fast",
              partition.getPartitionName(), lastTopStateInstance, curTopStateInstance));
      return;
    }

    long duration = endTime - startTime;
    long helixLatency = duration - fromTopStateUserLatency - toTopStateuserLatency;
    // We always treat such top state handoff as graceful as if top state handoff is triggered
    // by instance crash, we cannot observe the entire handoff process within 1 pipeline run
    logMissingTopStateInfo(duration, helixLatency, true, partition.getPartitionName());
    if (clusterStatusMonitor != null) {
      clusterStatusMonitor
          .updateMissingTopStateDurationStats(resourceName, duration, helixLatency, true, true);
    }
  }

  /**
   * Check if the given partition of the given resource has a missing top state duration larger
   * than the threshold, if so, report a top state transition failure
   *
   * @param cache cluster data cache
   * @param resourceName resource name
   * @param partition partition of the given resource
   * @param durationThreshold top state handoff duration threshold
   * @param clusterStatusMonitor monitor object
   */
  private void reportTopStateHandoffFailIfNecessary(ClusterDataCache cache, String resourceName,
      Partition partition, long durationThreshold, ClusterStatusMonitor clusterStatusMonitor) {
    Map<String, Map<String, MissingTopStateRecord>> missingTopStateMap =
        cache.getMissingTopStateMap();
    String partitionName = partition.getPartitionName();
    MissingTopStateRecord record = missingTopStateMap.get(resourceName).get(partitionName);
    long startTime = record.getStartTimeStamp();
    if (startTime > 0 && System.currentTimeMillis() - startTime > durationThreshold && !record
        .isFailed()) {
      record.setFailed();
      missingTopStateMap.get(resourceName).put(partitionName, record);
      if (clusterStatusMonitor != null) {
        clusterStatusMonitor.updateMissingTopStateDurationStats(resourceName, 0L, 0L, false, false);
      }
    }
  }

  /**
   * When we find a top state missing of the given partition, we find out when it started to miss
   * top state, then we record it in cache
   *
   * @param cache cluster data cache
   * @param resourceName resource name
   * @param partition partition of the given resource
   * @param topState top state name
   * @param currentStateOutput current state output
   */
  private void reportTopStateMissing(ClusterDataCache cache, String resourceName, Partition partition,
      String topState, CurrentStateOutput currentStateOutput) {
    Map<String, Map<String, MissingTopStateRecord>> missingTopStateMap = cache.getMissingTopStateMap();
    Map<String, Map<String, String>> lastTopStateMap = cache.getLastTopStateLocationMap();
    if (missingTopStateMap.containsKey(resourceName) && missingTopStateMap.get(resourceName)
        .containsKey(partition.getPartitionName())) {
      // a previous missing has been already recorded
      return;
    }

    long startTime = TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED;
    long fromTopStateUserLatency = DEFAULT_HANDOFF_USER_LATENCY;
    boolean isGraceful = true;

    // 1. try to find the previous topstate missing event for the startTime.
    String missingStateInstance = null;
    if (lastTopStateMap.containsKey(resourceName)) {
      missingStateInstance = lastTopStateMap.get(resourceName).get(partition.getPartitionName());
    }

    if (missingStateInstance != null) {
      Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
      if (liveInstances.containsKey(missingStateInstance)) {
        CurrentState currentState = cache.getCurrentState(missingStateInstance,
            liveInstances.get(missingStateInstance).getSessionId()).get(resourceName);

        if (currentState != null
            && currentState.getPreviousState(partition.getPartitionName()) != null && currentState
            .getPreviousState(partition.getPartitionName()).equalsIgnoreCase(topState)) {

          // Update the latest start time only from top state to other state transition
          // At beginning, the start time should -1 (not recorded). If something happen either
          // instance not alive or the instance just started for that partition, Helix does not know
          // the previous start time or end time. So we count from current.
          //
          // Previous state is top state does not mean that resource has only one top state
          // (i.e. Online/Offline). So Helix has to find the latest start time as the staring point.
          long fromTopStateStartTime = currentState.getStartTime(partition.getPartitionName());
          if (fromTopStateStartTime > startTime) {
            startTime = fromTopStateStartTime;
            fromTopStateUserLatency =
                currentState.getEndTime(partition.getPartitionName()) - startTime;
          }
          startTime = Math.max(startTime, currentState.getStartTime(partition.getPartitionName()));
        } // Else no related state transition history found, use current time as the missing start time.
      } else {
        // If the previous topState holder is no longer alive, the offline time is used as start time.
        // Also, if we observe a top state missing and the previous top state node is gone, the
        // top state handoff is not graceful
        isGraceful = false;
        Map<String, Long> offlineMap = cache.getInstanceOfflineTimeMap();
        if (offlineMap.containsKey(missingStateInstance)) {
          startTime = Math.max(startTime, offlineMap.get(missingStateInstance));
        }
      }
    }

    // 2. if no previous top state records, it's either resource just created or there is a
    // controller leadership change. Check any pending message that are created for top state
    // transition. Assume this is graceful top state handoff as if the from top state instance
    // crashed, we are not recording such message
    if (startTime == TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED) {
      for (Message message : currentStateOutput.getPendingMessageMap(resourceName, partition)
          .values()) {
        // Only messages that match the current session ID will be recorded in the map.
        // So no need to redundantly check here.
        if (message.getToState().equals(topState)) {
          startTime = Math.max(startTime, message.getCreateTimeStamp());
        }
      }
    }

    // 3. if no clue about previous top state or any related pending message, it could be
    //    a. resource just created
    //    b. controller leader switch (actual hand off could be either graceful or non graceful)
    //
    // Use the current system time as missing top state start time and assume always graceful
    // TODO: revise this case and see if this case can be better addressed
    if (startTime == TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED) {
      LogUtil.logWarn(LOG, _eventId,
          "Cannot confirm top state missing start time. Use the current system time as the start time.");
      startTime = System.currentTimeMillis();
    }

    if (!missingTopStateMap.containsKey(resourceName)) {
      missingTopStateMap.put(resourceName, new HashMap<String, MissingTopStateRecord>());
    }

    missingTopStateMap.get(resourceName).put(partition.getPartitionName(),
        new MissingTopStateRecord(startTime, fromTopStateUserLatency, isGraceful));
  }

  /**
   * When we see a top state come back, i.e. we observe a top state in this pipeline run,
   * but have a top state missing record before, we need to remove the top state missing
   * record and report top state handoff duration
   *
   * @param cache cluster data cache
   * @param stateMap state map of the given partition of the given resource
   * @param resourceName resource name
   * @param partition partition of the resource
   * @param clusterStatusMonitor monitor object
   * @param threshold top state handoff threshold
   * @param topState name of the top state
   */
  private void reportTopStateComesBack(ClusterDataCache cache, Map<String, String> stateMap, String resourceName,
      Partition partition, ClusterStatusMonitor clusterStatusMonitor, long threshold,
      String topState) {
    Map<String, Map<String, MissingTopStateRecord>> missingTopStateMap =
        cache.getMissingTopStateMap();
    MissingTopStateRecord record =
        missingTopStateMap.get(resourceName).get(partition.getPartitionName());
    long handOffStartTime = record.getStartTimeStamp();
    long fromTopStateUserLatency = record.getUserLatency();

    // Find the earliest end time from the top states and the corresponding user latency
    long handOffEndTime = Long.MAX_VALUE;
    long toTopStateUserLatency = DEFAULT_HANDOFF_USER_LATENCY;
    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    for (String instanceName : stateMap.keySet()) {
      CurrentState currentState =
          cache.getCurrentState(instanceName, liveInstances.get(instanceName).getSessionId())
              .get(resourceName);
      if (currentState.getState(partition.getPartitionName()).equalsIgnoreCase(topState)) {
        if (currentState.getEndTime(partition.getPartitionName()) <= handOffEndTime) {
          handOffEndTime = currentState.getEndTime(partition.getPartitionName());
          toTopStateUserLatency =
              handOffEndTime - currentState.getStartTime(partition.getPartitionName());
        }
      }
    }

    if (handOffStartTime > 0 && handOffEndTime - handOffStartTime <= threshold) {
      long duration = handOffEndTime - handOffStartTime;
      long helixLatency = duration - fromTopStateUserLatency - toTopStateUserLatency;
      // It is possible that during controller leader switch, we lost previous master information
      // and use current time to approximate missing top state start time. If we see the actual
      // user latency is larger than the duration we estimated, we use user latency to start with
      duration = Math.max(duration, helixLatency);
      boolean isGraceful = record.isGracefulHandoff();
      logMissingTopStateInfo(duration, helixLatency, isGraceful, partition.getPartitionName());

      if (clusterStatusMonitor != null) {
        clusterStatusMonitor
            .updateMissingTopStateDurationStats(resourceName, duration, helixLatency, isGraceful,
                true);
      }
    }
    removeFromStatsMap(missingTopStateMap, resourceName, partition);
  }

  private void removeFromStatsMap(
      Map<String, Map<String, MissingTopStateRecord>> missingTopStateMap, String resourceName,
      Partition partition) {
    if (missingTopStateMap.containsKey(resourceName)) {
      missingTopStateMap.get(resourceName).remove(partition.getPartitionName());
      if (missingTopStateMap.get(resourceName).isEmpty()) {
        missingTopStateMap.remove(resourceName);
      }
    }
  }

  private void logMissingTopStateInfo(long totalDuration, long userLatency, boolean isGraceful,
      String partitionName) {
    LogUtil.logInfo(LOG, _eventId, String
        .format("Missing top state duration is %s/%s for partition %s. Graceful: %s", userLatency,
            totalDuration, partitionName, isGraceful));
  }
}
