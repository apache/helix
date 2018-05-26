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

import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.*;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;


/**
 * For each LiveInstances select currentState and message whose sessionId matches
 * sessionId from LiveInstance Get Partition,State for all the resources computed in
 * previous State [ResourceComputationStage]
 */
public class CurrentStateComputationStage extends AbstractBaseStage {
  private static Logger LOG = LoggerFactory.getLogger(CurrentStateComputationStage.class);

  public final long NOT_RECORDED = -1L;
  public final long TRANSITION_FAILED = -2L;
  public final String TASK_STATE_MODEL_NAME = "Task";

  @Override
  public void process(ClusterEvent event) throws Exception {
    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
    final Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());

    if (cache == null || resourceMap == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires DataCache|RESOURCE");
    }

    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    final CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    for (LiveInstance instance : liveInstances.values()) {
      String instanceName = instance.getInstanceName();
      String instanceSessionId = instance.getSessionId();

      // update pending messages
      Map<String, Message> messages = cache.getMessages(instanceName);
      updatePendingMessages(instance, messages.values(), currentStateOutput, resourceMap);

      // update current states.
      Map<String, CurrentState> currentStateMap = cache.getCurrentState(instanceName, instanceSessionId);
      updateCurrentStates(instance, currentStateMap.values(), currentStateOutput, resourceMap);
    }

    if (!cache.isTaskCache()) {
      final ClusterStatusMonitor clusterStatusMonitor = event.getAttribute(AttributeName.clusterStatusMonitor.name());
      asyncExecute(cache.getAsyncTasksThreadPool(), new Callable<Object>() {
        @Override
        public Object call() {
          for (Resource resource : resourceMap.values()) {
            int totalPendingMessageCount = 0;
            for (Partition partition : resource.getPartitions()) {
              totalPendingMessageCount +=
                  currentStateOutput.getPendingMessageMap(resource.getResourceName(), partition).size();
            }
            clusterStatusMonitor.updatePendingMessages(resource.getResourceName(), totalPendingMessageCount);
          }
          return null;
        }
      });
      // TODO Update the status async -- jjwang
      updateTopStateStatus(cache, clusterStatusMonitor, resourceMap, currentStateOutput);
    }
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
  }

  // update all pending messages to CurrentStateOutput.
  private void updatePendingMessages(LiveInstance instance, Collection<Message> pendingMessages,
      CurrentStateOutput currentStateOutput, Map<String, Resource> resourceMap) {
    String instanceName = instance.getInstanceName();
    String instanceSessionId = instance.getSessionId();

    // update all pending messages
    for (Message message : pendingMessages) {
      if (!MessageType.STATE_TRANSITION.name().equalsIgnoreCase(message.getMsgType())
          && !MessageType.STATE_TRANSITION_CANCELLATION.name().equalsIgnoreCase(message.getMsgType())) {
        continue;
      }
      if (!instanceSessionId.equals(message.getTgtSessionId())) {
        continue;
      }
      String resourceName = message.getResourceName();
      Resource resource = resourceMap.get(resourceName);
      if (resource == null) {
        continue;
      }

      if (!message.getBatchMessageMode()) {
        String partitionName = message.getPartitionName();
        Partition partition = resource.getPartition(partitionName);
        if (partition != null) {
          setMessageState(currentStateOutput, resourceName, partition, instanceName, message);
        } else {
          // log
        }
      } else {
        List<String> partitionNames = message.getPartitionNames();
        if (!partitionNames.isEmpty()) {
          for (String partitionName : partitionNames) {
            Partition partition = resource.getPartition(partitionName);
            if (partition != null) {
              setMessageState(currentStateOutput, resourceName, partition, instanceName, message);
            } else {
              // log
            }
          }
        }
      }
    }
  }

  // update current states in CurrentStateOutput
  private void updateCurrentStates(LiveInstance instance, Collection<CurrentState> currentStates,
      CurrentStateOutput currentStateOutput, Map<String, Resource> resourceMap) {
    String instanceName = instance.getInstanceName();
    String instanceSessionId = instance.getSessionId();

    for (CurrentState currentState : currentStates) {
      if (!instanceSessionId.equals(currentState.getSessionId())) {
        continue;
      }
      String resourceName = currentState.getResourceName();
      String stateModelDefName = currentState.getStateModelDefRef();
      Resource resource = resourceMap.get(resourceName);
      if (resource == null) {
        continue;
      }
      if (stateModelDefName != null) {
        currentStateOutput.setResourceStateModelDef(resourceName, stateModelDefName);
      }

      currentStateOutput.setBucketSize(resourceName, currentState.getBucketSize());

      Map<String, String> partitionStateMap = currentState.getPartitionStateMap();
      for (String partitionName : partitionStateMap.keySet()) {
        Partition partition = resource.getPartition(partitionName);
        if (partition != null) {
          currentStateOutput.setCurrentState(resourceName, partition, instanceName,
              currentState.getState(partitionName));
          currentStateOutput.setRequestedState(resourceName, partition, instanceName,
              currentState.getRequestedState(partitionName));
          currentStateOutput
              .setInfo(resourceName, partition, instanceName, currentState.getInfo(partitionName));
          currentStateOutput.setEndTime(resourceName, partition, instanceName,
              currentState.getEndTime(partitionName));
        }
      }
    }
  }

  private void setMessageState(CurrentStateOutput currentStateOutput, String resourceName,
      Partition partition, String instanceName, Message message) {
    if (MessageType.STATE_TRANSITION.name().equalsIgnoreCase(message.getMsgType())) {
      currentStateOutput.setPendingState(resourceName, partition, instanceName, message);
    } else {
      currentStateOutput.setCancellationState(resourceName, partition, instanceName, message);
    }
  }

  private void updateTopStateStatus(ClusterDataCache cache,
      ClusterStatusMonitor clusterStatusMonitor, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput) {
    Map<String, Map<String, Long>> missingTopStateMap = cache.getMissingTopStateMap();
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
        // Resource does not have valid statemodel or it is task state model
        continue;
      }

      String resourceName = resource.getResourceName();

      for (Partition partition : resource.getPartitions()) {
        Map<String, String> stateMap =
            currentStateOutput.getCurrentStateMap(resourceName, partition);

        for (String instance : stateMap.keySet()) {
          if (stateMap.get(instance).equals(stateModelDef.getTopState())) {
            if (!lastTopStateMap.containsKey(resourceName)) {
              lastTopStateMap.put(resourceName, new HashMap<String, String>());
            }
            // recording any top state, it is enough for tracking the top state changes.
            lastTopStateMap.get(resourceName).put(partition.getPartitionName(), instance);
            break;
          }
        }

        if (stateMap.values().contains(stateModelDef.getTopState())) {
          // Top state comes back
          // The first time participant started or controller switched will be ignored
          reportTopStateComesBack(cache, stateMap, missingTopStateMap, resourceName, partition,
              clusterStatusMonitor, durationThreshold, stateModelDef.getTopState());
        } else {
          // TODO: improve following with MIN_ACTIVE_TOP_STATE logic
          // Missing top state need to record
          reportNewTopStateMissing(cache, missingTopStateMap, lastTopStateMap, resourceName,
              partition, stateModelDef.getTopState(), currentStateOutput);
        }
      }
    }

    // Check whether it is already passed threshold
    for (String resourceName : missingTopStateMap.keySet()) {
      for (String partitionName : missingTopStateMap.get(resourceName).keySet()) {
        long startTime = missingTopStateMap.get(resourceName).get(partitionName);
        if (startTime > 0 && System.currentTimeMillis() - startTime > durationThreshold) {
          missingTopStateMap.get(resourceName).put(partitionName, TRANSITION_FAILED);
          if (clusterStatusMonitor != null) {
            clusterStatusMonitor.updateMissingTopStateDurationStats(resourceName, 0L, false);
          }
        }
      }
    }
    if (clusterStatusMonitor != null) {
      clusterStatusMonitor.resetMaxMissingTopStateGauge();
    }
  }

  private void reportNewTopStateMissing(ClusterDataCache cache,
      Map<String, Map<String, Long>> missingTopStateMap,
      Map<String, Map<String, String>> lastTopStateMap, String resourceName, Partition partition,
      String topState, CurrentStateOutput currentStateOutput) {
    if (missingTopStateMap.containsKey(resourceName) && missingTopStateMap.get(resourceName)
        .containsKey(partition.getPartitionName())) {
      // a previous missing has been already recorded
      return;
    }

    long startTime = NOT_RECORDED;

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
          startTime = Math.max(startTime, currentState.getStartTime(partition.getPartitionName()));
        } // Else no related state transition history found, use current time as the missing start time.
      } else {
        // If the previous topState holder is no longer alive, the offline time is used as start time.
        Map<String, Long> offlineMap = cache.getInstanceOfflineTimeMap();
        if (offlineMap.containsKey(missingStateInstance)) {
          startTime = Math.max(startTime, offlineMap.get(missingStateInstance));
        }
      }
    }

    // 2. if no previous topstate records, use any pending message that are created for topstate transition
    if (startTime == NOT_RECORDED) {
      for (Message message : currentStateOutput.getPendingMessageMap(resourceName, partition)
          .values()) {
        // Only messages that match the current session ID will be recorded in the map.
        // So no need to redundantly check here.
        if (message.getToState().equals(topState)) {
          startTime = Math.max(startTime, message.getCreateTimeStamp());
        }
      }
    }

    // 3. if no clue about previous topstate or any related pending message, use the current system time.
    if (startTime == NOT_RECORDED) {
      LOG.warn("Cannot confirm top state missing start time. Use the current system time as the start time.");
      startTime = System.currentTimeMillis();
    }

    if (!missingTopStateMap.containsKey(resourceName)) {
      missingTopStateMap.put(resourceName, new HashMap<String, Long>());
    }

    Map<String, Long> partitionMap = missingTopStateMap.get(resourceName);
    // Update the new partition without top state
    if (!partitionMap.containsKey(partition.getPartitionName())) {
      partitionMap.put(partition.getPartitionName(), startTime);
    }
  }

  private void reportTopStateComesBack(ClusterDataCache cache, Map<String, String> stateMap,
      Map<String, Map<String, Long>> missingTopStateMap, String resourceName, Partition partition,
      ClusterStatusMonitor clusterStatusMonitor, long threshold, String topState) {
    if (!missingTopStateMap.containsKey(resourceName) || !missingTopStateMap.get(resourceName)
        .containsKey(partition.getPartitionName())) {
      // there is no previous missing recorded
      return;
    }

    long handOffStartTime = missingTopStateMap.get(resourceName).get(partition.getPartitionName());

    // Find the earliest end time from the top states
    long handOffEndTime = System.currentTimeMillis();
    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    for (String instanceName : stateMap.keySet()) {
      CurrentState currentState =
          cache.getCurrentState(instanceName, liveInstances.get(instanceName).getSessionId())
              .get(resourceName);
      if (currentState.getState(partition.getPartitionName()).equalsIgnoreCase(topState)) {
        handOffEndTime =
            Math.min(handOffEndTime, currentState.getEndTime(partition.getPartitionName()));
      }
    }

    if (handOffStartTime > 0 && handOffEndTime - handOffStartTime <= threshold) {
      LOG.info(String.format("Missing topstate duration is %d for partition %s",
          handOffEndTime - handOffStartTime, partition.getPartitionName()));
      if (clusterStatusMonitor != null) {
        clusterStatusMonitor
            .updateMissingTopStateDurationStats(resourceName, handOffEndTime - handOffStartTime,
                true);
      }
    }
    removeFromStatsMap(missingTopStateMap, resourceName, partition);
  }

  private void removeFromStatsMap(Map<String, Map<String, Long>> missingTopStateMap,
      String resourceName, Partition partition) {
    if (missingTopStateMap.containsKey(resourceName)) {
      missingTopStateMap.get(resourceName).remove(partition.getPartitionName());
    }

    if (missingTopStateMap.get(resourceName).size() == 0) {
      missingTopStateMap.remove(resourceName);
    }
  }
}
