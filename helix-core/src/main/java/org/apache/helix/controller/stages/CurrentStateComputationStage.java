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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());

    if (cache == null || resourceMap == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires DataCache|RESOURCE");
    }

    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

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
      ClusterStatusMonitor clusterStatusMonitor =
          event.getAttribute(AttributeName.clusterStatusMonitor.name());
      updateMissingTopStateStatus(cache, clusterStatusMonitor, resourceMap, currentStateOutput);
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
          currentStateOutput.setInfo(resourceName, partition, instanceName, currentState.getInfo(partitionName));
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
  private void updateMissingTopStateStatus(ClusterDataCache cache,
      ClusterStatusMonitor clusterStatusMonitor, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput) {
    Map<String, Map<String, Long>> missingTopStateMap = cache.getMissingTopStateMap();
    long durationThreshold = Long.MAX_VALUE;
    if (cache.getClusterConfig() != null) {
      durationThreshold = cache.getClusterConfig().getMissTopStateDurationThreshold();
    }

    for (Resource resource : resourceMap.values()) {
      StateModelDefinition stateModelDef = cache.getStateModelDef(resource.getStateModelDefRef());
      if (stateModelDef == null || resource.getStateModelDefRef()
          .equalsIgnoreCase(TASK_STATE_MODEL_NAME)) {
        // Resource does not have valid statemodel or it is task state model
        continue;
      }

      for (Partition partition : resource.getPartitions()) {
        Map<String, String> stateMap =
            currentStateOutput.getCurrentStateMap(resource.getResourceName(), partition);

        // TODO: improve following with MIN_ACTIVE_TOP_STATE logic
        // Missing top state need to record
        if (!stateMap.values().contains(stateModelDef.getTopState()) && (!missingTopStateMap
            .containsKey(resource.getResourceName()) || !missingTopStateMap
            .get(resource.getResourceName()).containsKey(partition.getPartitionName()))) {
          reportNewTopStateMissing(cache, stateMap, missingTopStateMap, resource, partition,
              stateModelDef.getTopState());
        }

        // Top state comes back
        // The first time participant started or controller switched will be ignored
        if (missingTopStateMap.containsKey(resource.getResourceName()) && missingTopStateMap
            .get(resource.getResourceName()).containsKey(partition.getPartitionName()) && stateMap
            .values().contains(stateModelDef.getTopState())) {
          reportTopStateComesBack(cache, stateMap, missingTopStateMap, resource, partition,
              clusterStatusMonitor, durationThreshold, stateModelDef.getTopState());
        }
      }
    }

    // Check whether it is already passed threshold
    for (String resourceName : missingTopStateMap.keySet()) {
      for (String partitionName : missingTopStateMap.get(resourceName).keySet()) {
        long startTime = missingTopStateMap.get(resourceName).get(partitionName);
        if (startTime > 0 && System.currentTimeMillis() - startTime > durationThreshold) {
          missingTopStateMap.get(resourceName).put(partitionName, TRANSITION_FAILED);
          clusterStatusMonitor
              .updateMissingTopStateDurationStats(resourceName, 0L, false);
        }
      }
    }
    if (clusterStatusMonitor != null) {
      clusterStatusMonitor.resetMaxMissingTopStateGauge();
    }
  }

  private void reportNewTopStateMissing(ClusterDataCache cache, Map<String, String> stateMap,
      Map<String, Map<String, Long>> missingTopStateMap, Resource resource, Partition partition,
      String topState) {

    long startTime = NOT_RECORDED;
    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    for (String instanceName : stateMap.keySet()) {
      if (liveInstances.containsKey(instanceName)) {
        CurrentState currentState =
            cache.getCurrentState(instanceName, liveInstances.get(instanceName).getSessionId())
                .get(resource.getResourceName());

        if (currentState.getPreviousState(partition.getPartitionName()) != null && currentState
            .getPreviousState(partition.getPartitionName()).equalsIgnoreCase(topState)) {
          // Update the latest start time only from top state to other state transition
          // At beginning, the start time should -1 (not recorded). If something happen either
          // instance not alive or the instance just started for that partition, Helix does not know
          // the previous start time or end time. So we count from current.
          //
          // Previous state is top state does not mean that resource has only one top state
          // (i.e. Online/Offline). So Helix has to find the latest start time as the staring point.
          startTime = Math.max(startTime, currentState.getStartTime(partition.getPartitionName()));
        }
      }
    }

    if (startTime == NOT_RECORDED) {
      startTime = System.currentTimeMillis();
    }

    if (!missingTopStateMap.containsKey(resource.getResourceName())) {
      missingTopStateMap.put(resource.getResourceName(), new HashMap<String, Long>());
    }

    Map<String, Long> partitionMap = missingTopStateMap.get(resource.getResourceName());
    // Update the new partition without top state
    if (!partitionMap.containsKey(partition.getPartitionName())) {
      missingTopStateMap.get(resource.getResourceName())
          .put(partition.getPartitionName(), startTime);
    }
  }

  private void reportTopStateComesBack(ClusterDataCache cache, Map<String, String> stateMap,
      Map<String, Map<String, Long>> missingTopStateMap, Resource resource, Partition partition,
      ClusterStatusMonitor clusterStatusMonitor, long threshold, String topState) {

    long handOffStartTime =
        missingTopStateMap.get(resource.getResourceName()).get(partition.getPartitionName());

    // Find the earliest end time from the top states
    long handOffEndTime = System.currentTimeMillis();
    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    for (String instanceName : stateMap.keySet()) {
      CurrentState currentState =
          cache.getCurrentState(instanceName, liveInstances.get(instanceName).getSessionId())
              .get(resource.getResourceName());
      if (currentState.getState(partition.getPartitionName()).equalsIgnoreCase(topState)) {
        handOffEndTime =
            Math.min(handOffEndTime, currentState.getEndTime(partition.getPartitionName()));
      }
    }

    if (handOffStartTime != TRANSITION_FAILED && handOffEndTime - handOffStartTime <= threshold) {
      LOG.info(String.format("Missing topstate duration is %d for partition %s",
          handOffEndTime - handOffStartTime, partition.getPartitionName()));
      clusterStatusMonitor.updateMissingTopStateDurationStats(resource.getResourceName(),
          handOffEndTime - handOffStartTime, true);
    }
    removeFromStatsMap(missingTopStateMap, resource, partition);
  }

  private void removeFromStatsMap(Map<String, Map<String, Long>> missingTopStateMap,
      Resource resource, Partition partition) {
    if (missingTopStateMap.containsKey(resource.getResourceName())) {
      missingTopStateMap.get(resource.getResourceName()).remove(partition.getPartitionName());
    }

    if (missingTopStateMap.get(resource.getResourceName()).size() == 0) {
      missingTopStateMap.remove(resource.getResourceName());
    }
  }
}
