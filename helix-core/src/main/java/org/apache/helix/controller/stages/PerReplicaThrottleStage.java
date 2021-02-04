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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
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
import org.apache.helix.model.StateModelDefinition;
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
    MessageOutput output = compute(event, resourceToRebalance, currentStateOutput, selectedMessages,
        retracedResourceStateMap);

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
   * @param currentStateOutput
   * @param selectedMessage
   * @param retracedResourceStateMap out
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

    List<ResourcePriority> prioritizedResourceList =
        getResourcePriorityList(resourceMap, dataCache);

    List<String> failedResources = new ArrayList<>();

    // Priority is applied in assignment computation
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
            throttlePerReplicaMessages(idealState, currentStateOutput, partitonMsgMap,
                bestPossibleStateOutput, dataCache, throttleController, retracedPartitionsState);
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
   * @param currentStateOutput
   * @param selectedResourceMessages
   * @param bestPossibleStateOutput
   * @param cache
   * @param throttleController
   * @param retracedPartitionsStateMap
   */
  private MessageOutput throttlePerReplicaMessages(IdealState idealState,
      CurrentStateOutput currentStateOutput, Map<Partition, List<Message>> selectedResourceMessages,
      BestPossibleStateOutput bestPossibleStateOutput, ResourceControllerDataProvider cache,
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

    Set<Partition> partitionsWithErrorStateReplica = new HashSet<>();

    Map<Partition, Map<String, Integer>> expectedStateCountByPartition = new HashMap<>();
    Map<Partition, Map<String, Integer>> currentStateCountsByPartition = new HashMap<>();

    calculateExistingAndCurrentStateCount(selectedResourceMessages, currentStateOutput,
        bestPossibleStateOutput, idealState, cache, expectedStateCountByPartition,
        currentStateCountsByPartition);

    // Step 1: charge existing pending messages and update retraced state map.
    // TODO: later PRs
    // Step 2: classify all the messages into recovery message list and load message list
    List<Message> recoveryMessages = new ArrayList<>();
    List<Message> loadMessages = new ArrayList<>();
    classifyMessages(resourceName, idealState, cache, selectedResourceMessages, recoveryMessages,
        loadMessages, expectedStateCountByPartition, currentStateCountsByPartition);

    // Step 3: sorts recovery message list and applies throttling
    Set<Message> throttledRecoveryMessages = new HashSet<>();
    LogUtil.logDebug(logger, _eventId,
        String.format("applying recovery rebalance with resource %s", resourceName));
    applyThrottling(resourceName, throttleController, idealState, cache, false, recoveryMessages,
        throttledRecoveryMessages, StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE);

    // Step 4: sorts load message list and applies throttling
    // TODO: calculate error-on-recovery downward threshold with complex backward compatibility next
    // TODO: this can be done together with chargePendingMessage() where partitionsNeedRecovery is from
    boolean onlyDownwardLoadBalance = partitionsWithErrorStateReplica.size() > 1;
    Set<Message> throttledLoadMessages = new HashSet<>();
    LogUtil.logDebug(logger, _eventId, String
        .format("applying load rebalance with resource %s, onlyDownwardLoadBalance %s",
            resourceName, onlyDownwardLoadBalance));
    applyThrottling(resourceName, throttleController, idealState, cache, onlyDownwardLoadBalance,
        loadMessages, throttledLoadMessages,
        StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE);

    LogUtil.logDebug(logger, _eventId, String
        .format("resource %s, throttled recovery message: %s", resourceName,
            throttledRecoveryMessages));
    LogUtil.logDebug(logger, _eventId, String
        .format("resource %s, throttled load messages: %s", resourceName, throttledLoadMessages));

    // Step 5: construct output
    for (Partition partition : selectedResourceMessages.keySet()) {
      List<Message> partitionMessages = selectedResourceMessages.get(partition);
      if (partitionMessages == null) {
        continue;
      }
      List<Message> finalPartitionMessages = new ArrayList<>();
      for (Message message : partitionMessages) {
        if (throttledRecoveryMessages.contains(message)) {
          continue;
        }
        if (throttledLoadMessages.contains(message)) {
          continue;
        }
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

  private void propagateCountsTopDown(StateModelDefinition stateModelDef,
      Map<String, Integer> expectedStateCountMap) {
    // attribute state in higher priority to lower priority
    List<String> stateList = stateModelDef.getStatesPriorityList();
    if (stateList.size() <= 0) {
      return;
    }
    int index = 0;
    String prevState = stateList.get(index);
    if (!expectedStateCountMap.containsKey(prevState)) {
      expectedStateCountMap.put(prevState, 0);
    }
    while (true) {
      if (index == stateList.size() - 1) {
        break;
      }
      index++;
      String curState = stateList.get(index);
      String num = stateModelDef.getNumInstancesPerState(curState);
      if ("-1".equals(num)) {
        break;
      }
      Integer prevCnt = expectedStateCountMap.get(prevState);
      expectedStateCountMap
          .put(curState, prevCnt + expectedStateCountMap.getOrDefault(curState, 0));
      prevState = curState;
    }
  }

  private void getPartitionExpectedAndCurrentStateCountMap(Partition partition,
      Map<String, List<String>> preferenceLists, IdealState idealState,
      ResourceControllerDataProvider cache, Map<String, String> currentStateMap,
      Map<String, Integer> expectedStateCountMapOut, Map<String, Integer> currentStateCountsOut) {
    List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
    if (preferenceList == null) {
      preferenceList = Collections.emptyList();
    }

    int replica =
        idealState.getMinActiveReplicas() == -1 ? idealState.getReplicaCount(preferenceList.size())
            : idealState.getMinActiveReplicas();
    Set<String> activeList = new HashSet<>(preferenceList);
    activeList.retainAll(cache.getEnabledLiveInstances());

    // For each state, check that this partition currently has the required number of that state as
    // required by StateModelDefinition.
    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    LinkedHashMap<String, Integer> expectedStateCountMap =
        stateModelDef.getStateCountMap(activeList.size(), replica); // StateModelDefinition's counts

    // Current counts without disabled partitions or disabled instances
    Map<String, String> currentStateMapWithoutDisabled = new HashMap<>(currentStateMap);
    currentStateMapWithoutDisabled.keySet().removeAll(cache
        .getDisabledInstancesForPartition(idealState.getResourceName(),
            partition.getPartitionName()));
    Map<String, Integer> currentStateCounts =
        StateModelDefinition.getStateCounts(currentStateMapWithoutDisabled);

    expectedStateCountMapOut.putAll(expectedStateCountMap);
    currentStateCountsOut.putAll(currentStateCounts);
    propagateCountsTopDown(stateModelDef, expectedStateCountMapOut);
    propagateCountsTopDown(stateModelDef, currentStateCountsOut);
  }

  void calculateExistingAndCurrentStateCount(Map<Partition, List<Message>> selectedResourceMessages,
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleStateOutput,
      IdealState idealState, ResourceControllerDataProvider cache,
      Map<Partition, Map<String, Integer>> expectedStateCountByPartition,
      Map<Partition, Map<String, Integer>> currentStateCountsByPartition) {
    String resourceName = idealState.getResourceName();
    Map<String, List<String>> preferenceLists =
        bestPossibleStateOutput.getPreferenceLists(resourceName);
    for (Partition partition : selectedResourceMessages.keySet()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);

      Map<String, Integer> expectedStateCounts = new HashMap<>();
      Map<String, Integer> currentStateCounts = new HashMap<>();
      getPartitionExpectedAndCurrentStateCountMap(partition, preferenceLists, idealState, cache,
          currentStateMap, expectedStateCounts, currentStateCounts);

      // save these two maps for later usage
      expectedStateCountByPartition.put(partition, expectedStateCounts);
      currentStateCountsByPartition.put(partition, currentStateCounts);
    }
  }

  /*
   * Classify the messages of each partition into recovery and load messages.
   */
  private void classifyMessages(String resourceName, IdealState idealState,
      ResourceControllerDataProvider cache, Map<Partition, List<Message>> selectedResourceMessages,
      List<Message> recoveryMessages, List<Message> loadMessages,
      Map<Partition, Map<String, Integer>> expectedStateCountByPartition,
      Map<Partition, Map<String, Integer>> currentStateCountsByPartition) {
    LogUtil.logInfo(logger, _eventId,
        String.format("Classify message for resource: %s", resourceName));

    for (Partition partition : selectedResourceMessages.keySet()) {
      Map<String, Integer> expectedStateCountMap = expectedStateCountByPartition.get(partition);
      Map<String, Integer> currentStateCounts = currentStateCountsByPartition.get(partition);

      List<Message> partitionMessages = selectedResourceMessages.get(partition);
      if (partitionMessages == null) {
        continue;
      }

      String stateModelDefName = idealState.getStateModelDefRef();
      StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
      // sort partitionMessages based on transition priority and then creation timestamp for transition message
      // TODO: sort messages in same partition in next PR
      Set<String> disabledInstances =
          cache.getDisabledInstancesForPartition(resourceName, partition.getPartitionName());
      for (Message msg : partitionMessages) {
        if (!Message.MessageType.STATE_TRANSITION.name().equals(msg.getMsgType())) {
          if (logger.isDebugEnabled()) {
            LogUtil.logDebug(logger, _eventId, String
                .format("Message: %s not subject to throttle in resource: %s with type %s", msg,
                    resourceName, msg.getMsgType()));
          }
          continue;
        }

        boolean isUpward = !isDownwardTransition(idealState, cache, msg);

        // for disabled disabled instance, the downward transition is not subjected to load throttling
        // we will let them pass through ASAP.
        String instance = msg.getTgtName();
        if (disabledInstances.contains(instance)) {
          if (!isUpward) {
            if (logger.isDebugEnabled()) {
              LogUtil.logDebug(logger, _eventId, String.format(
                  "Message: %s not subject to throttle in resource: %s to disabled instancce %s",
                  msg, resourceName, instance));
            }
            continue;
          }
        }

        String toState = msg.getToState();
        if (toState.equals(HelixDefinedState.DROPPED.name()) || toState
            .equals(HelixDefinedState.ERROR.name())) {
          if (logger.isDebugEnabled()) {
            LogUtil.logDebug(logger, _eventId, String
                .format("Message: %s not subject to throttle in resource: %s with toState %s", msg,
                    resourceName, toState));
          }
          continue;
        }

        Integer expectedCount = expectedStateCountMap.get(toState);
        Integer currentCount = currentStateCounts.get(toState);
        expectedCount = expectedCount == null ? 0 : expectedCount;
        currentCount = currentCount == null ? 0 : currentCount;

        if (isUpward && (currentCount < expectedCount)) {
          recoveryMessages.add(msg);
          currentStateCounts.put(toState, currentCount + 1);
        } else {
          loadMessages.add(msg);
        }
      }
    }
  }

  protected void applyThrottling(String resourceName,
      StateTransitionThrottleController throttleController, IdealState idealState,
      ResourceControllerDataProvider cache, boolean onlyDownwardLoadBalance, List<Message> messages,
      Set<Message> throttledMessages, StateTransitionThrottleConfig.RebalanceType rebalanceType) {
    boolean isRecovery =
        rebalanceType == StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE;
    if (isRecovery && onlyDownwardLoadBalance) {
      logger.error("onlyDownwardLoadBalance can't be used together with recovery_rebalance");
      return;
    }

    // TODO: add message sorting in next PR
    logger.trace("throttleControllerstate->{} before load", throttleController);
    for (Message msg : messages) {
      if (onlyDownwardLoadBalance) {
        if (!isDownwardTransition(idealState, cache, msg)) {
          throttledMessages.add(msg);
          if (logger.isDebugEnabled()) {
            LogUtil.logDebug(logger, _eventId, String
                .format("Message: %s throttled in resource as not downward: %s with type: %s", msg,
                    resourceName, rebalanceType));
          }
          continue;
        }
      }

      if (throttleController.shouldThrottleForResource(rebalanceType, resourceName)) {
        throttledMessages.add(msg);
        if (logger.isDebugEnabled()) {
          LogUtil.logDebug(logger, _eventId, String
              .format("Message: %s throttled in resource: %s with type: %s", msg, resourceName,
                  rebalanceType));
        }
        continue;
      }
      String instance = msg.getTgtName();
      if (throttleController.shouldThrottleForInstance(rebalanceType, instance)) {
        throttledMessages.add(msg);
        if (logger.isDebugEnabled()) {
          LogUtil.logDebug(logger, _eventId, String
              .format("Message: %s throttled in instance %s in resource: %s with type: %s",
                  instance, msg, resourceName, rebalanceType));
        }
        continue;
      }
      throttleController.chargeInstance(rebalanceType, instance);
      throttleController.chargeResource(rebalanceType, resourceName);
      throttleController.chargeCluster(rebalanceType);
      logger
          .trace("throttleControllerstate->{} after charge load msg: {}", throttleController, msg);
    }
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

  private boolean isDownwardTransition(IdealState idealState, ResourceControllerDataProvider cache,
      Message message) {
    boolean isDownward = false;

    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    Map<String, Integer> statePriorityMap = stateModelDef.getStatePriorityMap();
    String fromState = message.getFromState();
    String toState = message.getToState();
    if (statePriorityMap.containsKey(fromState) && statePriorityMap.containsKey(toState)) {
      // If the state is not found in statePriorityMap, consider it not strictly downward by
      // default because we can't determine whether it is downward
      if (statePriorityMap.get(fromState) < statePriorityMap.get(toState)) {
        isDownward = true;
      }
    }

    return isDownward;
  }
}