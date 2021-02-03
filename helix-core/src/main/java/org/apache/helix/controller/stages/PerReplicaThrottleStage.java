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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixException;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PerReplicaThrottleStage extends AbstractBaseStage {
  private static final Logger logger =
      LoggerFactory.getLogger(PerReplicaThrottleStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();

    CurrentStateOutput currentStateOutput = event.getAttribute(AttributeName.CURRENT_STATE.name());

    MessageOutput selectedMessages = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    LogUtil.logDebug(logger, _eventId, String.format("selectedMessages is: %s", selectedMessages));

    Map<String, Resource> resourceToRebalance =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    ResourceControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());

    if (currentStateOutput == null || selectedMessages == null || resourceToRebalance == null
        || cache == null) {
      throw new StageException(String.format("Missing attributes in event: %s. "
              + "Requires CURRENT_STATE (%s) |BEST_POSSIBLE_STATE (%s) |RESOURCES (%s) |DataCache (%s)",
          event, currentStateOutput, selectedMessages, resourceToRebalance, cache));
    }

    ResourcesStateMap retracedResourceStateMap = new ResourcesStateMap();
    MessageOutput output =
        compute(event, resourceToRebalance, selectedMessages, retracedResourceStateMap);

    event.addAttribute(AttributeName.PER_REPLICA_THROTTLE_OUTPUT_MESSAGES.name(), output);
    LogUtil.logDebug(logger, _eventId,
        String.format("retraceResourceStateMap is: %s", retracedResourceStateMap));
    event.addAttribute(AttributeName.PER_REPLICA_THROTTLE_RETRACED_STATES.name(),
        retracedResourceStateMap);

    //TODO: enter maintenance mode logic in next PR
  }

  private List<ResourcePriority> getResourcePriorityList(Map<String, Resource> resourceMap,
      ResourceControllerDataProvider dataCache) {
    List<ResourcePriority> prioritizedResourceList = new ArrayList<>();
    resourceMap.keySet().stream().forEach(
        resourceName -> prioritizedResourceList.add(new ResourcePriority(resourceName, dataCache)));
    Collections.sort(prioritizedResourceList);

    return prioritizedResourceList;
  }

  /**
   * Go through each resource, and based on messageSelected and currentState, compute
   * messageOutput while maintaining throttling constraints (for example, ensure that the number
   * of possible pending state transitions does NOT go over the set threshold).
   * @param event
   * @param resourceMap
   * @param selectedMessage
   * @param retracedResourceStateMap out
   */
  private MessageOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      MessageOutput selectedMessage, ResourcesStateMap retracedResourceStateMap) {
    MessageOutput output = new MessageOutput();

    ResourceControllerDataProvider dataCache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());

    StateTransitionThrottleController throttleController =
        new StateTransitionThrottleController(resourceMap.keySet(), dataCache.getClusterConfig(),
            dataCache.getLiveInstances().keySet());

    List<ResourcePriority> prioritizedResourceList =
        getResourcePriorityList(resourceMap, dataCache);

    List<String> failedResources = new ArrayList<>();

    // Priority is applied in assignment computation because higher priority by looping in order of
    // decreasing priority
    for (ResourcePriority resourcePriority : prioritizedResourceList) {
      String resourceName = resourcePriority.getResourceName();

      BestPossibleStateOutput bestPossibleStateOutput =
          event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
      if (!bestPossibleStateOutput.containsResource(resourceName)) {
        LogUtil.logInfo(logger, _eventId, String.format(
            "Skip calculating per replica state for resource %s because the best possible state is not available.",
            resourceName));
        continue;
      }

      Resource resource = resourceMap.get(resourceName);
      IdealState idealState = dataCache.getIdealState(resourceName);
      if (idealState == null) {
        // If IdealState is null, use an empty one
        LogUtil.logInfo(logger, _eventId, String
            .format("IdealState for resource %s does not exist; resource may not exist anymore",
                resourceName));
        idealState = new IdealState(resourceName);
        idealState.setStateModelDefRef(resource.getStateModelDefRef());
      }

      Map<Partition, Map<String, String>> retracedPartitionsState = new HashMap<>();
      try {
        Map<Partition, List<Message>> partitonMsgMap = new HashMap<>();
        for (Partition partition : resource.getPartitions()) {
          List<Message> msgList = selectedMessage.getMessages(resourceName, partition);
          partitonMsgMap.put(partition, msgList);
        }
        MessageOutput resourceMsgOut =
            throttlePerReplicaMessages(idealState, partitonMsgMap, bestPossibleStateOutput,
                throttleController, retracedPartitionsState);
        for (Partition partition : resource.getPartitions()) {
          List<Message> msgList = resourceMsgOut.getMessages(resourceName, partition);
          output.addMessages(resourceName, partition, msgList);
        }
        retracedResourceStateMap.setState(resourceName, retracedPartitionsState);
      } catch (HelixException ex) {
        LogUtil.logInfo(logger, _eventId,
            "Failed to calculate per replica partition states for resource " + resourceName, ex);
        failedResources.add(resourceName);
      }
    }

    // TODO: add monitoring in next PR.
    return output;
  }

  /*
   * Apply per-replica throttling logic and filter out excessive recovery and load messages for a
   * given resource.
   * Reconstruct retrace partition states for a resource based on pending and targeted messages
   * Return messages for partitions of a resource.
   * @param idealState
   * @param selectedResourceMessages
   * @param bestPossibleStateOutput
   * @param throttleController
   * @param retracedPartitionsStateMap
   */
  private MessageOutput throttlePerReplicaMessages(IdealState idealState,
      Map<Partition, List<Message>> selectedResourceMessages,
      BestPossibleStateOutput bestPossibleStateOutput,
      StateTransitionThrottleController throttleController,
      Map<Partition, Map<String, String>> retracedPartitionsStateMap) {
    MessageOutput output = new MessageOutput();
    String resourceName = idealState.getResourceName();
    LogUtil.logInfo(logger, _eventId, String.format("Processing resource: %s", resourceName));

    // TODO: expand per-replica-throttling beyond FULL_AUTO
    if (!throttleController.isThrottleEnabled() || !IdealState.RebalanceMode.FULL_AUTO
        .equals(idealState.getRebalanceMode())) {
      retracedPartitionsStateMap
          .putAll(bestPossibleStateOutput.getPartitionStateMap(resourceName).getStateMap());
      for (Partition partition : selectedResourceMessages.keySet()) {
        output.addMessages(resourceName, partition, selectedResourceMessages.get(partition));
      }
      return output;
    }

    // TODO: later PRs
    // Step 1: charge existing pending messages and update retraced state map.
    // Step 2: classify all the messages into recovery message list and load message list
    // Step 3: sorts recovery message list and applies throttling
    // Step 4: sorts load message list and applies throttling
    // Step 5: construct output
    for (Partition partition : selectedResourceMessages.keySet()) {
      List<Message> partitionMessages = selectedResourceMessages.get(partition);
      if (partitionMessages == null) {
        continue;
      }
      List<Message> finalPartitionMessages = new ArrayList<>();
      for (Message message : partitionMessages) {
        // TODO: next PR messages exclusion
        finalPartitionMessages.add(message);
      }
      output.addMessages(resourceName, partition, finalPartitionMessages);
    }
    // Step 6: constructs all retraced partition state map for the resource
    // TODO: next PR
    // Step 7: emit metrics
    // TODO: next PR
    return output;
  }

  // ------------------ utilities ---------------------------

  /**
   * POJO that maps resource name to its priority represented by an integer.
   */
  private static class ResourcePriority implements Comparable<ResourcePriority> {
    private String _resourceName;
    private int _priority;

    ResourcePriority(String resourceName, ResourceControllerDataProvider dataCache) {
      // Resource level prioritization based on the numerical (sortable) priority field.
      // If the resource priority field is null/not set, the resource will be treated as lowest
      // priority.
      _priority = Integer.MIN_VALUE;
      _resourceName = resourceName;
      String priorityField = dataCache.getClusterConfig().getResourcePriorityField();
      if (priorityField != null) {
        // Will take the priority from ResourceConfig first
        // If ResourceConfig does not exist or does not have this field.
        // Try to load it from the resource's IdealState. Otherwise, keep it at the lowest priority
        ResourceConfig config = dataCache.getResourceConfig(resourceName);
        IdealState idealState = dataCache.getIdealState(resourceName);
        if (config != null && config.getSimpleConfig(priorityField) != null) {
          this.setPriority(config.getSimpleConfig(priorityField));
        } else if (idealState != null
            && idealState.getRecord().getSimpleField(priorityField) != null) {
          this.setPriority(idealState.getRecord().getSimpleField(priorityField));
        }
      }
    }

    @Override
    public int compareTo(ResourcePriority resourcePriority) {
      // make sure larger _priority is in front of small _priority at sort time
      return Integer.compare(resourcePriority._priority, _priority);
    }

    public String getResourceName() {
      return _resourceName;
    }

    public void setPriority(String priority) {
      try {
        _priority = Integer.parseInt(priority);
      } catch (Exception e) {
        logger.warn(
            String.format("Invalid priority field %s for resource %s", priority, _resourceName));
      }
    }
  }
}