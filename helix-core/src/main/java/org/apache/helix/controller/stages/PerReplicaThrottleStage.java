package org.apache.helix.controller.stages;

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
        compute(event, resourceToRebalance, currentStateOutput, selectedMessages, retracedResourceStateMap);

    event.addAttribute(AttributeName.PER_REPLICA_OUTPUT_MESSAGES.name(), output);
    LogUtil.logDebug(logger,_eventId, String.format("retraceResourceStateMap is: %s", retracedResourceStateMap));
    event.addAttribute(AttributeName.PER_REPLICA_RETRACED_STATES.name(), retracedResourceStateMap);

    //TODO: enter maintenance mode logic in next PR
  }

  private List<ResourcePriority> getResourcePriorityList(
      Map<String, Resource> resourceMap,
      ResourceControllerDataProvider dataCache) {
    List<ResourcePriority> prioritizedResourceList = new ArrayList<>();
    for (String resourceName : resourceMap.keySet()) {
      prioritizedResourceList.add(new ResourcePriority(resourceName, dataCache));
    }
    Collections.sort(prioritizedResourceList);

    return prioritizedResourceList;
  }

  /**
   * Go through each resource, and based on messageSelected and currentState, compute
   * messageOutput while maintaining throttling constraints (for example, ensure that the number
   * of possible pending state transitions does NOT go over the set threshold).
   * @param event
   * @param resourceMap
   * @param currentStateOutput
   * @param selectedMessage
   * @param retracedResourceStateMap out
   * @return
   */
  private MessageOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput, MessageOutput selectedMessage,
      ResourcesStateMap retracedResourceStateMap) {
    MessageOutput output = new MessageOutput();

    ResourceControllerDataProvider dataCache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());

    StateTransitionThrottleController throttleController =
        new StateTransitionThrottleController(resourceMap.keySet(), dataCache.getClusterConfig(),
            dataCache.getLiveInstances().keySet());

    List<ResourcePriority> prioritizedResourceList = getResourcePriorityList(resourceMap, dataCache);

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
          List<Message> msgList = selectedMessage.getMessages(resource.getResourceName(), partition);
          partitonMsgMap.put(partition, msgList);
        }
        throttlePerReplicaMessages(idealState, currentStateOutput,
            partitonMsgMap, resourceMap.get(resourceName),
            bestPossibleStateOutput, dataCache, throttleController, retracedPartitionsState,
            output);
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
   * @param currentStateOutput
   * @param selectedResourceMessages
   * @param resource
   * @param bestPossibleStateOutput
   * @param cache
   * @param throttleController
   * @param retracedPartitionsStateMap
   * @param throttledRecoveryMsgOut
   * @param throttledLoadMessageOut
   * @param output
   * @return
   */
  private void throttlePerReplicaMessages(IdealState idealState,
      CurrentStateOutput currentStateOutput, Map<Partition, List<Message>> selectedResourceMessages,
      Resource resource, BestPossibleStateOutput bestPossibleStateOutput,
      ResourceControllerDataProvider cache, StateTransitionThrottleController throttleController,
      Map<Partition, Map<String, String>> retracedPartitionsStateMap,
      MessageOutput output) {
    String resourceName = resource.getResourceName();
    LogUtil.logInfo(logger, _eventId, String.format("Processing resource: %s", resourceName));

    // TODO: expand per-replica-throttling beyond FULL_AUTO
    if (!throttleController.isThrottleEnabled() || !IdealState.RebalanceMode.FULL_AUTO
        .equals(idealState.getRebalanceMode())) {
      retracedPartitionsStateMap.putAll(bestPossibleStateOutput.getPartitionStateMap(resourceName).getStateMap());
      for (Partition partition : selectedResourceMessages.keySet()) {
        output.addMessages(resourceName, partition, selectedResourceMessages.get(partition));
      }
      return;
    }

    Map<Partition, Map<String, Integer>> expectedStateCountByPartition = new HashMap<>();
    Map<Partition, Map<String, Integer>> currentStateCountsByPartition = new HashMap<>();
    // TODO: later PRs
    // Step 1: charge existing pending messages and update retraced state map.
    // Step 2: classify all the messages into recovery message list and load message list
    // Step 3: sorts recovery message list and applies throttling
    // Step 4: sorts load message list and applies throttling
    // Step 5: construct output
    Map<Partition, List<Message>> outMessagesByPartition = new HashMap<>();
    for (Partition partition : resource.getPartitions()) {
      List<Message> partitionMessages = selectedResourceMessages.get(partition);
      if (partitionMessages == null) {
        continue;
      }
      List<Message> finalPartitionMessages = new ArrayList<>();
      for (Message message: partitionMessages) {
        // TODO: next PR messages exclusion
        finalPartitionMessages.add(message);
      }
      outMessagesByPartition.put(partition, finalPartitionMessages);
      output.addMessages(resourceName, partition, finalPartitionMessages);
    }
    // Step 6: constructs all retraced partition state map for the resource
    // TODO: next PR
    // Step 7: emit metrics
    // TODO: next PR
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
        if (dataCache.getResourceConfig(resourceName) != null
            && dataCache.getResourceConfig(resourceName).getSimpleConfig(priorityField) != null) {
          this.setPriority(
              dataCache.getResourceConfig(resourceName).getSimpleConfig(priorityField));
        } else if (dataCache.getIdealState(resourceName) != null
            && dataCache.getIdealState(resourceName).getRecord().getSimpleField(priorityField)
            != null) {
          this.setPriority(
              dataCache.getIdealState(resourceName).getRecord().getSimpleField(priorityField));
        }
      }
    }

    ResourcePriority(String resourceName, int priority) {
      _resourceName = resourceName;
      _priority = priority;
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
