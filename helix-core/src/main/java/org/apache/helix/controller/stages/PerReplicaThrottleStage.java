package org.apache.helix.controller.stages;

import java.util.ArrayList;
import java.util.Comparator;
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
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.ResourceMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PerReplicaThrottleStage extends AbstractBaseStage {
  private static final Logger logger =
      LoggerFactory.getLogger(PerReplicaThrottleStage.class.getName());

  public PerReplicaThrottleStage() {
  }

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
    List<Message> throttledRecoveryMsg = new ArrayList<>();
    List<Message> throttledLoadMsg = new ArrayList<>();
    MessageOutput output =
        compute(event, resourceToRebalance, currentStateOutput, selectedMessages, retracedResourceStateMap, throttledRecoveryMsg, throttledLoadMsg);

    event.addAttribute(AttributeName.PER_REPLICA_OUTPUT_MESSAGES.name(), output);
    LogUtil.logDebug(logger,_eventId, String.format("retraceResourceStateMap is: %s", retracedResourceStateMap));
    event.addAttribute(AttributeName.PER_REPLICA_RETRACED_STATES.name(), retracedResourceStateMap);

    //TODO: enter maintenance mode logic
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
      ResourcesStateMap retracedResourceStateMap,
      List<Message> throttledRecoveryMsg, List<Message> throttledLoadMsg) {
    MessageOutput output = new MessageOutput();

    ResourceControllerDataProvider dataCache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());

    StateTransitionThrottleController throttleController =
        new StateTransitionThrottleController(resourceMap.keySet(), dataCache.getClusterConfig(),
            dataCache.getLiveInstances().keySet());

    // Resource level prioritization based on the numerical (sortable) priority field.
    // If the resource priority field is null/not set, the resource will be treated as lowest
    // priority.
    List<ResourcePriority> prioritizedResourceList = new ArrayList<>();
    for (String resourceName : resourceMap.keySet()) {
      prioritizedResourceList.add(new ResourcePriority(resourceName, Integer.MIN_VALUE));
    }
    // If resourcePriorityField is null at the cluster level, all resources will be considered equal
    // in priority by keeping all priorities at MIN_VALUE
    String priorityField = dataCache.getClusterConfig().getResourcePriorityField();
    if (priorityField != null) {
      for (ResourcePriority resourcePriority : prioritizedResourceList) {
        String resourceName = resourcePriority.getResourceName();

        // Will take the priority from ResourceConfig first
        // If ResourceConfig does not exist or does not have this field.
        // Try to load it from the resource's IdealState. Otherwise, keep it at the lowest priority
        if (dataCache.getResourceConfig(resourceName) != null
            && dataCache.getResourceConfig(resourceName).getSimpleConfig(priorityField) != null) {
          resourcePriority.setPriority(
              dataCache.getResourceConfig(resourceName).getSimpleConfig(priorityField));
        } else if (dataCache.getIdealState(resourceName) != null
            && dataCache.getIdealState(resourceName).getRecord().getSimpleField(priorityField)
            != null) {
          resourcePriority.setPriority(
              dataCache.getIdealState(resourceName).getRecord().getSimpleField(priorityField));
        }
      }
      prioritizedResourceList.sort(new ResourcePriorityComparator());
    }

    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
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
        throttlePerReplicaMessages(idealState, clusterStatusMonitor, currentStateOutput,
            selectedMessage.getResourceMessages(resourceName), resourceMap.get(resourceName),
            bestPossibleStateOutput, dataCache, throttleController, retracedPartitionsState,
            throttledRecoveryMsg, throttledLoadMsg, output);
        retracedResourceStateMap.setState(resourceName, retracedPartitionsState);
      } catch (HelixException ex) {
        LogUtil.logInfo(logger, _eventId,
            "Failed to calculate per replica partition states for resource " + resourceName, ex);
        failedResources.add(resourceName);
      }
    }

    if (clusterStatusMonitor != null) {
      clusterStatusMonitor.setResourceRebalanceStates(failedResources,
          ResourceMonitor.RebalanceStatus.PER_REPLICA_STATE_CAL_FAILED);
      clusterStatusMonitor.setResourceRebalanceStates(output.resourceSet(),
          ResourceMonitor.RebalanceStatus.NORMAL);
    }

    return output;
  }

  /*
   * Apply per-replica throttling logic and filter out excessive recovery and load messages for a
   * given resource.
   * Reconstruct retrace partition states for a resource based on pending and targeted messages
   * Return messages for partitions of a resource.
   * Out param retracedPartitionsCurrentState
   * Out param output
   */
  private void throttlePerReplicaMessages(IdealState idealState,
      ClusterStatusMonitor clusterStatusMonitor,
      CurrentStateOutput currentStateOutput, Map<Partition, List<Message>> selectedResourceMessages,
      Resource resource, BestPossibleStateOutput bestPossibleStateOutput,
      ResourceControllerDataProvider cache, StateTransitionThrottleController throttleController,
      Map<Partition, Map<String, String>> retracedPartitionsStateMap,
      List<Message> throttledRecoveryMsgOut, List<Message> throttledLoadMessageOut,
      MessageOutput output) {
    String resourceName = resource.getResourceName();
    LogUtil.logInfo(logger, _eventId, String.format("Processing resource: %s", resourceName));

    if (!throttleController.isThrottleEnabled() || !IdealState.RebalanceMode.FULL_AUTO
        .equals(idealState.getRebalanceMode())) {
      retracedPartitionsStateMap.putAll(bestPossibleStateOutput.getPartitionStateMap(resourceName).getStateMap());
      for (Partition partition : selectedResourceMessages.keySet()) {
        output.addMessages(resourceName, partition, selectedResourceMessages.get(partition));
      }
      return;
    }
    // TODO:
    // Step 1: charge existing pending messages and update retraced state map.
    // Step 2: classify all the messages into recovery message list and load message list
    // Step 3: sorts recovery message list and applies throttling
    // Step 4: sorts load message list and applies throttling
    // Step 5: construct output
    // Step 6: constructs all retraced partition state map for the resource
    // Step 7: emit metrics
  }

  // ------------------ utilities ---------------------------
  /**
   * POJO that maps resource name to its priority represented by an integer.
   */
  private static class ResourcePriority {
    private String _resourceName;
    private int _priority;

    ResourcePriority(String resourceName, Integer priority) {
      _resourceName = resourceName;
      _priority = priority;
    }

    public int compareTo(ResourcePriority resourcePriority) {
      return Integer.compare(_priority, resourcePriority._priority);
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

  private static class ResourcePriorityComparator implements Comparator<ResourcePriority> {
    @Override
    public int compare(ResourcePriority priority1, ResourcePriority priority2) {
      return priority2.compareTo(priority1);
    }
  }
}
