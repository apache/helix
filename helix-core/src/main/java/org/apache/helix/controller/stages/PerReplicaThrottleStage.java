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

import org.apache.helix.HelixException;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
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
    logger.debug("event info: {}, selectedMessages is: {}", _eventId, selectedMessages);

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
    logger.debug("Event info: {}, retraceResourceStateMap is: {}", retracedResourceStateMap);
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
        logger.info(
            "Event info: {}, Skip calculating per replica state for resource {} because the best possible state is not available.",
            _eventId, resourceName);
        continue;
      }

      Resource resource = resourceMap.get(resourceName);
      IdealState idealState = dataCache.getIdealState(resourceName);
      if (idealState == null) {
        // If IdealState is null, use an empty one
        logger.info(
            "Event info: {}, IdealState for resource {} does not exist; resource may not exist anymore.",
            _eventId, resourceName);
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
        logger.info(
            "Event info: {}, Failed to calculate per replica partition states for resource {} ",
            _eventId, resourceName, ex);
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
    logger.info("Event info: {}, Processing resource: {}", _eventId, resourceName);

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

    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);

    Map<Partition, Map<String, Integer>> expectedStateCountByPartition =
        calculateExpectedStateCounts(selectedResourceMessages, bestPossibleStateOutput, idealState,
            cache.getEnabledLiveInstances(), stateModelDef);
    Map<Partition, Map<String, Integer>> currentStateCountsByPartition =
        calculateCurrentStateCount(selectedResourceMessages, currentStateOutput, stateModelDef,
            resourceName, cache);

    // Step 1: charge existing pending messages and update retraced state map.
    // TODO: later PRs
    // Step 2: classify all the messages into recovery message list and load message list
    List<Message> recoveryMessages = new ArrayList<>();
    List<Message> loadMessages = new ArrayList<>();

    classifyMessages(resourceName, stateModelDef, cache, selectedResourceMessages, recoveryMessages,
        loadMessages, expectedStateCountByPartition, currentStateCountsByPartition);

    // Step 3: sorts recovery message list and applies throttling
    Set<Message> throttledRecoveryMessages = new HashSet<>();
    logger.debug("Event info {}, applying recovery rebalance with resource {}", _eventId,
        resourceName);
    applyThrottling(resourceName, throttleController, stateModelDef, false, recoveryMessages,
        throttledRecoveryMessages, StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE);

    // Step 4: sorts load message list and applies throttling
    // TODO: calculate error-on-recovery downward threshold with complex backward compatibility next
    // TODO: this can be done together with chargePendingMessage() where partitionsNeedRecovery is from
    boolean onlyDownwardLoadBalance = partitionsWithErrorStateReplica.size() > 1;
    Set<Message> throttledLoadMessages = new HashSet<>();
    logger.debug(
        "Event info {}, applying load rebalance with resource {}, onlyDownwardLoadBalance {} ",
        _eventId, resourceName, onlyDownwardLoadBalance);
    applyThrottling(resourceName, throttleController, stateModelDef, onlyDownwardLoadBalance,
        loadMessages, throttledLoadMessages,
        StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE);

    logger
        .debug("Event info {}, resource {}, throttled recovery message {}", _eventId, resourceName,
            throttledRecoveryMessages);
    logger.debug("Event info {}, resource {}, throttled load message {}", _eventId, resourceName,
        throttledLoadMessages);

    // Step 5: construct output
    for (Partition partition : selectedResourceMessages.keySet()) {
      List<Message> partitionMessages = selectedResourceMessages.get(partition);
      if (partitionMessages == null) {
        continue;
      }
      List<Message> finalPartitionMessages = new ArrayList<>();
      for (Message message : partitionMessages) {
        if (throttledRecoveryMessages.contains(message) || throttledLoadMessages
            .contains(message)) {
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

  /*
   *  Let us use an example to illustrate the state count accumulation idea:
   *  Currentstate N1(O), N2(S), N3(O), message N3(O->S) should be classified as recovery.
   *  Assuming without the propagateCounts logic,
   *  Here, the expectedStateCounts is {M->1, S->1, O->0}, given MIN_ACTIVE=2.
   *  The currentStateCounts is {M->0, S->1, O->0}.
   *  At the time, message N3(O->S) is tested for load/recovery, the logic would compare
   *  currentStateCounts[S] with expectedStateCounts[S]. If less than expected, it is deemed as
   *  recovery; otherwise load.
   *  Then the message would be incorrectly classified as load.
   *
   *  With propogationTopDown, we have expectedStateCounts as {M->1, S->2 O->3}.
   *  currentStateCountCounts as {M->0, S->1, O->1}. Thus the message is going to be classified
   *  as recovery correctly.
   *
   *  The gist is that:
   *  When determining a message as LOAD or RECOVERY, we look at the toState of this message.
   *  If the accumulated current count of toState meet the required accumulated expected count
   *  of the toState, we will treat it as Load, otherwise, it is Recovery.
   *
   *  Note, there can be customerized model that having more than one route to a top state. For
   *  example S1, S2, S3 are three levels of states with S1 as top state with lowest priority.
   *  It is possible that S2 can transits upward to S1 while S3 can also transits upward to S1.
   *  Thus, we consider S1 meet count requirement of both S2 and S3. In propagation time, we will
   *  add state count of S1 to both S2 and S3.
   */
  private void propagateCountsTopDown(StateModelDefinition stateModelDef,
      Map<String, Integer> stateCountMap) {
    List<String> stateList = stateModelDef.getStatesPriorityList();
    if (stateList == null || stateList.size() <= 0) {
      return;
    }

    Map<String, Integer> statePriorityMap = new HashMap<>();

    // calculate rank of each state. Next use the rank to compare if a transition is upward or not
    int rank = 0;
    for (String state : stateList) {
      statePriorityMap.put(state, Integer.valueOf(rank));
      rank++;
    }

    // given a key state, find the set of states that can transit to this key state upwards.
    Map<String, Set<String>> fromStatesMap = new HashMap<>();
    for (String transition : stateModelDef.getStateTransitionPriorityList()) {
      // Note, we assume stateModelDef is properly constructed.
      String[] fromStateAndToState = transition.split("-");
      String fromState = fromStateAndToState[0];
      String toState = fromStateAndToState[1];
      Integer fromStatePriority = statePriorityMap.get(fromState);
      Integer toStatePriority = statePriorityMap.get(toState);
      if (fromStatePriority.compareTo(toStatePriority) <= 0) {
        // skip downward transitition
        continue;
      }
      fromStatesMap.putIfAbsent(toState, new HashSet<>());
      fromStatesMap.get(toState).add(fromState);
    }

    // propagation by adding state counts of current state to all lower priority state that can
    // transit to this current state
    int index = 0;
    while (true) {
      if (index == stateList.size() - 1) {
        break;
      }
      String curState = stateList.get(index);
      String num = stateModelDef.getNumInstancesPerState(curState);
      if ("-1".equals(num)) {
        break;
      }
      stateCountMap.putIfAbsent(curState, 0);
      Integer curCount = stateCountMap.get(curState);
      // for all states S that can transition to curState, add curState count back to S in stateCountMap
      for (String fromState : fromStatesMap.getOrDefault(curState, Collections.emptySet())) {
        Integer fromStateCount = stateCountMap.getOrDefault(fromState, 0);
        stateCountMap.put(fromState, Integer.sum(fromStateCount, curCount));
      }
      index++;
    }
  }

  Map<String, Integer> getPartitionExpectedStateCounts(Partition partition,
      Map<String, List<String>> preferenceLists, StateModelDefinition stateModelDef,
      IdealState idealState, Set<String> enabledLiveInstance) {
    Map<String, Integer> expectedStateCountsOut = new HashMap<>();

    List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
    if (preferenceList == null) {
      preferenceList = Collections.emptyList();
    }

    int replica =
        idealState.getMinActiveReplicas() <= 0 ? idealState.getReplicaCount(preferenceList.size())
            : idealState.getMinActiveReplicas();
    Set<String> activeList = new HashSet<>(preferenceList);
    activeList.retainAll(enabledLiveInstance);

    // For each state, check that this partition currently has the required number of that state as
    // required by StateModelDefinition.
    LinkedHashMap<String, Integer> expectedStateCountMap =
        stateModelDef.getStateCountMap(activeList.size(), replica);

    expectedStateCountsOut.putAll(expectedStateCountMap);
    propagateCountsTopDown(stateModelDef, expectedStateCountsOut);

    return expectedStateCountsOut;
  }

  Map<Partition, Map<String, Integer>> calculateExpectedStateCounts(
      Map<Partition, List<Message>> selectedResourceMessages,
      BestPossibleStateOutput bestPossibleStateOutput, IdealState idealState,
      Set<String> enabledLiveInstance, StateModelDefinition stateModelDef) {
    Map<Partition, Map<String, Integer>> expectedStateCountsByPartition = new HashMap<>();

    String resourceName = idealState.getResourceName();
    Map<String, List<String>> preferenceLists =
        bestPossibleStateOutput.getPreferenceLists(resourceName);
    for (Partition partition : selectedResourceMessages.keySet()) {
      Map<String, Integer> expectedStateCounts =
          getPartitionExpectedStateCounts(partition, preferenceLists, stateModelDef, idealState,
              enabledLiveInstance);
      expectedStateCountsByPartition.put(partition, expectedStateCounts);
    }

    return expectedStateCountsByPartition;
  }

  Map<String, Integer> getPartitionCurrentStateCounts(Map<String, String> currentStateMap,
      String resourceName, String partitionName, StateModelDefinition stateModelDef,
      ResourceControllerDataProvider cache) {
    // Current counts without disabled partitions or disabled instances
    Map<String, String> currentStateMapWithoutDisabled = new HashMap<>(currentStateMap);
    currentStateMapWithoutDisabled.keySet()
        .removeAll(cache.getDisabledInstancesForPartition(resourceName, partitionName));
    Map<String, Integer> currentStateCountsOut =
        StateModelDefinition.getStateCounts(currentStateMapWithoutDisabled);

    propagateCountsTopDown(stateModelDef, currentStateCountsOut);

    return currentStateCountsOut;
  }

  Map<Partition, Map<String, Integer>> calculateCurrentStateCount(
      Map<Partition, List<Message>> selectedResourceMessages, CurrentStateOutput currentStateOutput,
      StateModelDefinition stateModelDef, String resourceName,
      ResourceControllerDataProvider cache) {
    Map<Partition, Map<String, Integer>> currentStateCountsByPartition = new HashMap<>();

    for (Partition partition : selectedResourceMessages.keySet()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);

      Map<String, Integer> currentStateCounts =
          getPartitionCurrentStateCounts(currentStateMap, resourceName,
              partition.getPartitionName(), stateModelDef, cache);
      currentStateCountsByPartition.put(partition, currentStateCounts);
    }

    return currentStateCountsByPartition;
  }

  /*
   * Classify the messages of each partition into recovery and load messages.
   */
  private void classifyMessages(String resourceName, StateModelDefinition stateModelDef,
      ResourceControllerDataProvider cache, Map<Partition, List<Message>> selectedResourceMessages,
      List<Message> recoveryMessages, List<Message> loadMessages,
      Map<Partition, Map<String, Integer>> expectedStateCountByPartition,
      Map<Partition, Map<String, Integer>> currentStateCountsByPartition) {
    logger.info("Event info {}, Classify message for resource {} ", _eventId, resourceName);

    for (Partition partition : selectedResourceMessages.keySet()) {
      Map<String, Integer> partitionExpectedStateCounts =
          expectedStateCountByPartition.get(partition);
      Map<String, Integer> partitionCurrentStateCounts =
          currentStateCountsByPartition.get(partition);

      List<Message> partitionMessages = selectedResourceMessages.get(partition);
      if (partitionMessages == null) {
        continue;
      }

      // sort partitionMessages based on transition priority and then creation timestamp for transition message
      // TODO: sort messages in same partition in next PR
      Set<String> disabledInstances =
          cache.getDisabledInstancesForPartition(resourceName, partition.getPartitionName());
      for (Message msg : partitionMessages) {
        if (!Message.MessageType.STATE_TRANSITION.name().equals(msg.getMsgType())) {
          logger.debug(
              "Event info: {} Message: {} not subject to throttle in resource: {} with type {}",
              _eventId, msg, resourceName, msg.getMsgType());
          continue;
        }

        boolean isUpward = !isDownwardTransition(stateModelDef, msg);

        // TODO: add disabled disabled instance special treatment

        String toState = msg.getToState();

        // TODO: dropped and Error state special treatment

        Integer minimumRequiredCount = partitionExpectedStateCounts.getOrDefault(toState, 0);
        Integer currentCount = partitionCurrentStateCounts.getOrDefault(toState, 0);

        //
        if (isUpward && (currentCount < minimumRequiredCount)) {
          recoveryMessages.add(msg);
          // It is critical to increase toState value by one here. For example, current state
          // of 3 replica in a partition is (M, O, O). Two messages here bringing up the two O to S.
          // In this case, the first O->S would be classified as recovery. Then this line would
          // increase the S state in partitionCurrentStateCounts value by one. Next O->S message
          // would be correctly marked as load message.
          partitionCurrentStateCounts.put(toState, currentCount + 1);
        } else {
          loadMessages.add(msg);
        }
      }
    }
  }

  protected void applyThrottling(String resourceName,
      StateTransitionThrottleController throttleController, StateModelDefinition stateModelDef,
      boolean onlyDownwardLoadBalance, List<Message> messages, Set<Message> throttledMessages,
      StateTransitionThrottleConfig.RebalanceType rebalanceType) {
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
        if (!isDownwardTransition(stateModelDef, msg)) {
          throttledMessages.add(msg);
          logger.debug(
              "Event info: {}, Message: {} throttled in resource as not downward: {} with type: {}",
              _eventId, msg, resourceName, rebalanceType);
          continue;
        }
      }

      if (throttleController.shouldThrottleForResource(rebalanceType, resourceName)) {
        throttledMessages.add(msg);
        logger
            .debug("Event info: {}, Message: {} throttled in resource: {} with type: {}", _eventId,
                msg, resourceName, rebalanceType);
        continue;
      }
      String instance = msg.getTgtName();
      if (throttleController.shouldThrottleForInstance(rebalanceType, instance)) {
        throttledMessages.add(msg);
        logger.debug(
            "Event info: {}, Message: {} throttled in instance {} in resource: {} with type: {} ",
            _eventId, msg, instance, resourceName, rebalanceType);
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

  private boolean isDownwardTransition(StateModelDefinition stateModelDef, Message message) {
    boolean isDownward = false;

    Map<String, Integer> statePriorityMap = stateModelDef.getStatePriorityMap();
    String fromState = message.getFromState();
    String toState = message.getToState();

    // Only when both fromState and toState can be found in the statePriorityMap, comparision of
    // state priority is used to determine if the transition is downward. Otherwise, we can't
    // really determine the order and by default consider the transition not downard.
    if (statePriorityMap.containsKey(fromState) && statePriorityMap.containsKey(toState)) {
      if (statePriorityMap.get(fromState) < statePriorityMap.get(toState)) {
        isDownward = true;
      }
    }

    return isDownward;
  }
}