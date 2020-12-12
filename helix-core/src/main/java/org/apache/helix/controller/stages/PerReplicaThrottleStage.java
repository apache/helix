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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PerReplicaThrottleStage extends AbstractBaseStage {
  private static final Logger logger =
      LoggerFactory.getLogger(PerReplicaThrottleStage.class.getName());

  private boolean isEmitThrottledMsg = false;

  public PerReplicaThrottleStage() {
    this(false);
  }

  protected PerReplicaThrottleStage(boolean enableEmitThrottledMsg) {
    isEmitThrottledMsg = enableEmitThrottledMsg;
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

    if (logger.isDebugEnabled()) {
      LogUtil.logDebug(logger, _eventId, String.format("output is"));
      for (String resource : resourceToRebalance.keySet()) {
        if (output.getResourceMessages(resource) != null) {
          LogUtil.logDebug(logger, _eventId, String.format("resource: %s", resource));
          Map<Partition, List<Message>> partitionListMap = output.getResourceMessages(resource);
          for (Partition partition : partitionListMap.keySet()) {
            for (Message msg : partitionListMap.get(partition)) {
              LogUtil.logDebug(logger, _eventId, String
                  .format("\tresource: %s, partition: %s,  msg: %s", resource, partition, msg));
            }
          }
        }
      }
    }
    event.addAttribute(AttributeName.PER_REPLICA_OUTPUT_MESSAGES.name(), output);
    LogUtil.logDebug(logger,_eventId, String.format("retraceResourceStateMap is: %s", retracedResourceStateMap));
    event.addAttribute(AttributeName.PER_REPLICA_RETRACED_STATES.name(), retracedResourceStateMap);

    if (isEmitThrottledMsg) {
      event.addAttribute(AttributeName.PER_REPLICA_THROTTLED_RECOVERY_MESSAGES.name(), throttledRecoveryMsg);
      event.addAttribute(AttributeName.PER_REPLICA_THOTTLED_LOAD_MESSAGES.name(), throttledLoadMsg);
    }

    // Make sure no instance has more replicas/partitions assigned than maxPartitionPerInstance. If
    // it does, pause the rebalance and put the cluster on maintenance mode
    int maxPartitionPerInstance = cache.getClusterConfig().getMaxPartitionsPerInstance();
    if (maxPartitionPerInstance > 0) {
      validateMaxPartitionsPerInstance(retracedResourceStateMap, maxPartitionPerInstance, cache, event);
    }

    //TODO: add metrics
  }

  /**
   * Go through every instance in the assignment and check that each instance does NOT have more
   * replicas for partitions assigned to it than maxPartitionsPerInstance. If the assignment
   * violates this, put the cluster on maintenance mode.
   * @param retracedResourceStateMap
   * @param maxPartitionPerInstance
   */
  private void validateMaxPartitionsPerInstance(ResourcesStateMap retracedResourceStateMap,
      int maxPartitionPerInstance,  ResourceControllerDataProvider cache, ClusterEvent event) {
    Map<String, PartitionStateMap> resourceStatesMap = retracedResourceStateMap.getResourceStatesMap();
    Map<String, Integer> instancePartitionCounts = new HashMap<>();

    for (String resource : resourceStatesMap.keySet()) {
      IdealState idealState = cache.getIdealState(resource);
      if (idealState != null
          && idealState.getStateModelDefRef().equals(BuiltInStateModelDefinitions.Task.name())) {
        // Ignore task here. Task has its own throttling logic
        continue;
      }

      PartitionStateMap partitionStateMap = resourceStatesMap.get(resource);
      Map<Partition, Map<String, String>> stateMaps = partitionStateMap.getStateMap();
      for (Partition p : stateMaps.keySet()) {
        Map<String, String> stateMap = stateMaps.get(p);
        for (String instance : stateMap.keySet()) {
          // If this replica is in DROPPED state, do not count it in the partition count since it is
          // to be dropped
          String state = stateMap.get(instance);
          if (state.equals(HelixDefinedState.DROPPED.name())) {
            continue;
          }
          if (!instancePartitionCounts.containsKey(instance)) {
            instancePartitionCounts.put(instance, 0);
          }
          int partitionCount = instancePartitionCounts.get(instance);
          // Number of replicas (from different partitions) held in this instance
          partitionCount++;
          if (partitionCount > maxPartitionPerInstance) {
            HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
            String errMsg = String.format(
                "Problem: according to this assignment, instance %s contains more "
                    + "replicas/partitions than the maximum number allowed (%d). Pipeline will "
                    + "stop the rebalance and put the cluster %s into maintenance mode",
                instance, maxPartitionPerInstance, cache.getClusterName());
            if (manager != null) {
              if (manager.getHelixDataAccessor()
                  .getProperty(manager.getHelixDataAccessor().keyBuilder().maintenance()) == null) {
                manager.getClusterManagmentTool().autoEnableMaintenanceMode(
                    manager.getClusterName(), true, errMsg,
                    MaintenanceSignal.AutoTriggerReason.MAX_PARTITION_PER_INSTANCE_EXCEEDED);
              }
              LogUtil.logWarn(logger, _eventId, errMsg);
            } else {
              LogUtil.logError(logger, _eventId,
                  "HelixManager is not set/null! Failed to pause this cluster/enable maintenance"
                      + " mode due to an instance being assigned more replicas/partitions than "
                      + "the limit.");
            }
            throw new HelixException(errMsg);
          }
          instancePartitionCounts.put(instance, partitionCount);
        }
      }
    }
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
        Map<Partition, List<Message>> resourceMessages =
            computeReourcePartitionState(idealState, currentStateOutput,
                selectedMessage.getResourceMessages(resourceName), resourceMap.get(resourceName),
                bestPossibleStateOutput, dataCache,
                throttleController, retracedPartitionsState, throttledRecoveryMsg, throttledLoadMsg);
        output.addResourceMessages(resourceName, resourceMessages);
        retracedResourceStateMap.setState(resourceName, retracedPartitionsState);
      } catch (HelixException ex) {
        LogUtil.logInfo(logger, _eventId,
            "Failed to calculate per replica partition states for resource " + resourceName, ex);
        failedResources.add(resourceName);
      }
    }

    return output;
  }

  /*
   * Apply per-replica throttling logic and filter out excessive recovery and load messages for a
   * given resource.
   * Reconstruct retrace partition states for a resource based on pending and targeted messages
   * Return messages for partitions of a resource.
   * Out param retracedPartitionsCurrentState
   */
  private Map<Partition, List<Message>> computeReourcePartitionState(IdealState idealState,
      CurrentStateOutput currentStateOutput, Map<Partition, List<Message>> selectedResourceMessages,
      Resource resource, BestPossibleStateOutput bestPossibleStateOutput,
      ResourceControllerDataProvider cache, StateTransitionThrottleController throttleController,
      Map<Partition, Map<String, String>> retracedPartitionsStateMap,
      List<Message> throttledRecoveryMsgOut, List<Message> throttledLoadMessageOut) {
    String resourceName = resource.getResourceName();
    LogUtil.logInfo(logger, _eventId, String.format("Processing resource: %s", resourceName));

    if (!throttleController.isThrottleEnabled() || !IdealState.RebalanceMode.FULL_AUTO
        .equals(idealState.getRebalanceMode())) {
      retracedPartitionsStateMap.putAll(bestPossibleStateOutput.getPartitionStateMap(resourceName).getStateMap());
      return selectedResourceMessages;
    }
    Map<String, List<String>> preferenceLists =
        bestPossibleStateOutput.getPreferenceLists(resourceName);

    Set<Partition> partitionsWithErrorStateReplica = new HashSet<>();
    Set<Partition> partitionsNeedRecovery = new HashSet<>();

    // Step 1: charge existing pending messages and update retraced state map.
    chargePendingMessages(resource, throttleController, currentStateOutput, bestPossibleStateOutput,
        idealState, cache, partitionsNeedRecovery, partitionsWithErrorStateReplica,
        retracedPartitionsStateMap);

    // Step 2: classify all the messages into recovery message list and load message list
    List<Message> recoveryMessages = new ArrayList<>();
    List<Message> loadMessages = new ArrayList<>();
    Map<Message, Partition> messagePartitionMap = new HashMap<>(); // todo: Message  may need a hashcode()
    classifyMessages(resource, currentStateOutput, bestPossibleStateOutput, idealState, cache,
        selectedResourceMessages, recoveryMessages, loadMessages, messagePartitionMap);

    // Step 3: sorts recovery message list and applies throttling
    Set<Message> throttledRecoveryMessages = new HashSet<>();

    Map<Partition, Map<String, String>> bestPossibleMap =
        bestPossibleStateOutput.getPartitionStateMap(resourceName).getStateMap();
    Map<Partition, Map<String, String>> currentStateMap =
        currentStateOutput.getCurrentStateMap(resourceName);
    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    LogUtil.logDebug(logger, _eventId,
        String.format("applying recovery rebalance with resource %s", resourceName));
    applyThrottling(resource, throttleController, currentStateMap, bestPossibleMap, idealState,
        cache, false, recoveryMessages, messagePartitionMap,
        throttledRecoveryMessages, StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE);

    // Step 4: sorts load message list and applies throttling

    // calculate error-on-recovery downward flag
    // If the threshold (ErrorOrRecovery) is set, then use it, if not, then check if the old
    // threshold (Error) is set. If the old threshold is set, use it. If not, use the default value
    // for the new one. This is for backward-compatibility
    int threshold = 1; // Default threshold for ErrorOrRecoveryPartitionThresholdForLoadBalance
    int partitionCount = partitionsWithErrorStateReplica.size();
    ClusterConfig clusterConfig = cache.getClusterConfig();
    if (clusterConfig.getErrorOrRecoveryPartitionThresholdForLoadBalance() != -1) {
      // ErrorOrRecovery is set
      threshold = clusterConfig.getErrorOrRecoveryPartitionThresholdForLoadBalance();
      partitionCount += partitionsNeedRecovery.size(); // Only add this count when the threshold is set
    } else {
      if (clusterConfig.getErrorPartitionThresholdForLoadBalance() != 0) {
        // 0 is the default value so the old threshold has been set
        threshold = clusterConfig.getErrorPartitionThresholdForLoadBalance();
      }
    }

    // Perform regular load balance only if the number of partitions in recovery and in error is
    // less than the threshold. Otherwise, only allow downward-transition load balance
    boolean onlyDownwardLoadBalance = partitionCount > threshold;
    Set<Message> throttledLoadMessages = new HashSet<>();
    LogUtil.logDebug(logger, _eventId,
        String.format("applying load rebalance with resource %s, onlyDownwardLoadBalance %s",
            resourceName, onlyDownwardLoadBalance));
    applyThrottling(resource, throttleController, currentStateMap, bestPossibleMap, idealState,
        cache, onlyDownwardLoadBalance, loadMessages, messagePartitionMap, throttledLoadMessages,
        StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE);

    LogUtil.logDebug(logger, _eventId,
        String.format("resource %s, throttled recovery message: %s", resourceName,throttledRecoveryMessages));
    LogUtil.logDebug(logger, _eventId,
        String.format("resource %s, throttled load messages: %s", resourceName,throttledLoadMessages));

    throttledRecoveryMsgOut.addAll(throttledRecoveryMessages);
    throttledLoadMessageOut.addAll(throttledLoadMessages);

    // Step 5: construct output
    Map<Partition, List<Message>> out = new HashMap<>();
    for (Partition partition : resource.getPartitions()) {
      List<Message> partitionMessages = selectedResourceMessages.get(partition);
      if (partitionMessages == null) {
        continue;
      }
      List<Message> finalPartitionMessages = new ArrayList<>();
      for (Message message: partitionMessages) {
        if (throttledRecoveryMessages.contains(message)) {
          continue;
        }
        if (throttledLoadMessages.contains(message)) {
          continue;
        }
        finalPartitionMessages.add(message);
      }
      out.put(partition, finalPartitionMessages);
    }

    // Step 6: constructs all retraced partition state map for the resource;
    constructRetracedPartitionStateMap(resource, retracedPartitionsStateMap, out);
    return out;
  }

  private void constructRetracedPartitionStateMap(Resource resource,
      Map<Partition, Map<String, String>> retracedPartitionsStateMap,
      Map<Partition, List<Message>> out) {
    for (Partition partition : resource.getPartitions()) {
      List<Message> partitionMessages = out.get(partition);
      if (partitionMessages == null) {
        continue;
      }
      for (Message message : partitionMessages) {
        if (!Message.MessageType.STATE_TRANSITION.name().equals(message.getMsgType())) {
          // todo: log?
          // ignore cancellation message etc.
          continue;
        }
        String toState = message.getToState();
        // toIntance may not be in the retracedStateMap as so far it is current state based.
        // new instance in best possible not in currentstate would not be in retracedStateMap yet.
        String toInstance = message.getTgtName();
        Map<String, String> retracedStateMap = retracedPartitionsStateMap.get(partition);
        retracedStateMap.put(toInstance, toState);
      }
    }
  }

  private void getPartitionExpectedAndCurrentStateCountMap(
      Partition partition,
      Map<String, List<String>> preferenceLists,
      IdealState idealState,
      ResourceControllerDataProvider cache,
      Map<String, String> currentStateMap,
      Map<String, Integer> expectedStateCountMapOut,
      Map<String, Integer> currentStateCountsOut
      ) {
    List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
    int replica = idealState.getMinActiveReplicas() == -1 ? idealState
        .getReplicaCount(preferenceList.size()) : idealState.getMinActiveReplicas();
    Set<String> activeList = new HashSet<>(preferenceList);
    activeList.retainAll(cache.getEnabledLiveInstances());

    // For each state, check that this partition currently has the required number of that state as
    // required by StateModelDefinition.
    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    LinkedHashMap<String, Integer> expectedStateCountMap = stateModelDef
        .getStateCountMap(activeList.size(), replica); // StateModelDefinition's counts

    // Current counts without disabled partitions or disabled instances
    Map<String, String> currentStateMapWithoutDisabled = new HashMap<>(currentStateMap);
    currentStateMapWithoutDisabled.keySet().removeAll(cache
        .getDisabledInstancesForPartition(idealState.getResourceName(),
            partition.getPartitionName()));
    Map<String, Integer> currentStateCounts =
        StateModelDefinition.getStateCounts(currentStateMapWithoutDisabled);

    expectedStateCountMapOut.putAll(expectedStateCountMap);
    currentStateCountsOut.putAll(currentStateCounts);
  }

  /*
   * Charge pending messages with recovery or load rebalance and update the retraced partition map
   * accordingly.
   * Also update partitionsNeedRecovery, partitionsWithErrorStateReplica accordingly which is used
   * by later steps.
   */
  private void chargePendingMessages(Resource resource,
      StateTransitionThrottleController throttleController,
      CurrentStateOutput currentStateOutput,
      BestPossibleStateOutput bestPossibleStateOutput,
      IdealState idealState,
      ResourceControllerDataProvider cache,
      Set<Partition> partitionsNeedRecovery,
      Set<Partition> partitionsWithErrorStateReplica,
      Map<Partition, Map<String, String>> retracedPartitionsStateMap) {

    logger.trace("throttleControllerstate->{} before pending message", throttleController);
    String resourceName = resource.getResourceName();
    Map<String, List<String>> preferenceLists =
        bestPossibleStateOutput.getPreferenceLists(resourceName);

    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);
      Map<String, String> retracedStateMap = new HashMap<>(currentStateMap);

      if (currentStateMap.values().contains(HelixDefinedState.ERROR.name())) {
        partitionsWithErrorStateReplica.add(partition);
      }

      Map<String, Integer> expectedStateCountMap = new HashMap<>();
      Map<String, Integer> currentStateCounts = new HashMap<>();
      getPartitionExpectedAndCurrentStateCountMap(partition, preferenceLists, idealState,
          cache, currentStateMap, expectedStateCountMap, currentStateCounts);

      Map<String, Message> pendingMessageMap =
          currentStateOutput.getPendingMessageMap(resourceName, partition);
      List<Message> pendingMessages = new ArrayList<>(pendingMessageMap.values());
      String stateModelDefName = idealState.getStateModelDefRef();
      StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);

      // sort pendingMessages based on transition priority then timeStamp for state transition message
      pendingMessages.sort(new PartitionMessageComparator(stateModelDef));
      List<Message> recoveryMessages = new ArrayList<>();
      List<Message> loadMessages = new ArrayList<>();
      for (Message msg : pendingMessages) {
        if (!Message.MessageType.STATE_TRANSITION.name().equals(msg.getMsgType())) {
          // ignore cancellation message etc. For now, don't charge them.
          continue;
        }
        String toState = msg.getToState();
        if (toState.equals(HelixDefinedState.DROPPED.name()) || toState
            .equals(HelixDefinedState.ERROR.name())) {
          continue;
        }

        Integer expectedCount = expectedStateCountMap.get(toState);
        Integer currentCount = currentStateCounts.get(toState);
        expectedCount = expectedCount == null ? 0 : expectedCount;
        currentCount = currentCount == null ? 0 : currentCount;

        boolean isUpward = !isDownwardTransition(idealState, cache, msg);
        // the gist is that if there is a topState, we should deem the topState also satisfy as secondTopState requirement.
        // upward AND (condition 1 or condition 2)
        // condition1: currentCount < expectedCount
        // condition2: currentCount == expected && toState is secondary state && currentCount(topState) < expectedCount(topState)
        String topState = stateModelDef.getTopState();
        String secondTopState = stateModelDef.getStatesPriorityList().get(1);
        Integer expectedTopCount = expectedStateCountMap.get(topState);
        Integer currentTopCount = currentStateCounts.get(topState);
        currentTopCount = currentTopCount == null ? 0 : currentTopCount;
        expectedTopCount = expectedTopCount == null ? 0 : expectedTopCount;

        if (isUpward && ((currentCount < expectedCount) || (currentCount == expectedCount && toState
            .equals(secondTopState) && currentTopCount < expectedTopCount))) {
          recoveryMessages.add(msg);
          partitionsNeedRecovery.add(partition);
          // update
          currentStateCounts.put(toState, currentCount + 1);
        } else {
          loadMessages.add(msg);
        }
      }
      // charge recovery message and retrace
      for (Message recoveryMsg : recoveryMessages) {
        String toState = recoveryMsg.getToState();
        String toInstance = recoveryMsg.getTgtName();
        // toInstance should be in currentStateMap
        retracedStateMap.put(toInstance, toState);

        throttleController
            .chargeInstance(StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
                toInstance);
        throttleController
            .chargeCluster(StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE);
        throttleController
            .chargeResource(StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
                resourceName);
        logger.trace("throttleControllerstate->{} after pending recovery charge msg:{}", throttleController, recoveryMsg);
      }
      // charge load message and retrace;
      // note if M->S with relay message, we don't charge relay message now. We would charge relay
      // message only when it shows in pending messages in the next cycle of controller run.
      for (Message loadMsg : loadMessages) {
        String toState = loadMsg.getToState();
        String toInstance = loadMsg.getTgtName();
        retracedStateMap.put(toInstance, toState);

        throttleController
            .chargeInstance(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE, toInstance);
        throttleController.chargeCluster(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE);
        throttleController
            .chargeResource(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE, resourceName);
        logger.trace("throttleControllerstate->{} after pending load charge msg:{}", throttleController, loadMsg);
      }
      retracedPartitionsStateMap.put(partition, retracedStateMap);
    }

  }

  /*
   * Classify the messages of each partition into recovery and load messages.
   */
  private void classifyMessages(Resource resource, CurrentStateOutput currentStateOutput,
      BestPossibleStateOutput bestPossibleStateOutput, IdealState idealState,
      ResourceControllerDataProvider cache, Map<Partition, List<Message>> selectedResourceMessages,
      List<Message> recoveryMessages, List<Message> loadMessages,
      Map<Message, Partition> messagePartitionMap) {

    String resourceName = resource.getResourceName();
    Map<String, List<String>> preferenceLists =
        bestPossibleStateOutput.getPreferenceLists(resourceName);
    LogUtil.logInfo(logger, _eventId, String.format("Classify message for resource: %s", resourceName));

    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);
      Map<String, Integer> expectedStateCountMap = new HashMap<>();
      Map<String, Integer> currentStateCounts = new HashMap<>();

      getPartitionExpectedAndCurrentStateCountMap(partition, preferenceLists, idealState,
          cache, currentStateMap, expectedStateCountMap, currentStateCounts);

      List<Message> partitionMessages = selectedResourceMessages.get(partition);
      if (partitionMessages == null) {
        continue;
      }

      String stateModelDefName = idealState.getStateModelDefRef();
      StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
      // sort partitionMessages based on transition priority and then creation timestamp for transition message
      partitionMessages.sort(new PartitionMessageComparator(stateModelDef));
      Set<String> disabledInstances =
          cache.getDisabledInstancesForPartition(resourceName, partition.getPartitionName());
      for (Message msg : partitionMessages) {
        if (!Message.MessageType.STATE_TRANSITION.name().equals(msg.getMsgType())) {
          if (logger.isDebugEnabled()) {
            LogUtil.logDebug(logger, _eventId,
                String.format("Message: %s not subject to throttle in resource: %s with type %s",
                    msg, resourceName, msg.getMsgType()));
          }
          continue;
        }

        messagePartitionMap.put(msg, partition);
        boolean isUpward = !isDownwardTransition(idealState, cache, msg);

        // for disabled disabled instance, the downward transition is not subjected to load throttling
        // we will let them pass through ASAP.
        String instance = msg.getTgtName();
        if (disabledInstances.contains(instance)) {
          if (!isUpward) {
            if (logger.isDebugEnabled()) {
              LogUtil.logDebug(logger, _eventId,
                  String.format("Message: %s not subject to throttle in resource: %s to disabled instancce %s",
                      msg, resourceName, instance));
            }
            continue;
          }
        }

        String toState = msg.getToState();
        if (toState.equals(HelixDefinedState.DROPPED.name()) || toState
            .equals(HelixDefinedState.ERROR.name())) {
          if (logger.isDebugEnabled()) {
            LogUtil.logDebug(logger, _eventId,
                String.format("Message: %s not subject to throttle in resource: %s with toState %s",
                    msg, resourceName, toState));
          }
          continue;
        }

        Integer expectedCount = expectedStateCountMap.get(toState);
        Integer currentCount = currentStateCounts.get(toState);
        expectedCount = expectedCount == null ? 0 : expectedCount;
        currentCount = currentCount == null ? 0 : currentCount;

        String topState = stateModelDef.getTopState();
        String secondTopState = stateModelDef.getStatesPriorityList().get(1);
        Integer expectedTopCount = expectedStateCountMap.get(topState);
        Integer currentTopCount = currentStateCounts.get(topState);
        currentTopCount = currentTopCount == null ? 0 : currentTopCount;
        expectedTopCount = expectedTopCount == null ? 0 : expectedTopCount;

        if (isUpward && ((currentCount < expectedCount) || (currentCount == expectedCount && toState
            .equals(secondTopState) && currentTopCount < expectedTopCount))) {
          recoveryMessages.add(msg);
          currentStateCounts.put(toState, currentCount + 1);
        } else {
          loadMessages.add(msg);
        }
      }
    }
  }

  private void applyThrottling(Resource resource,
      StateTransitionThrottleController throttleController,
      Map<Partition, Map<String, String>> currentStateMap,
      Map<Partition, Map<String, String>> bestPossibleMap,
      IdealState idealState,
      ResourceControllerDataProvider cache,
      boolean onlyDownwardLoadBalance,
      List<Message> messages,
      Map<Message, Partition> messagePartitionMap,
      Set<Message> throttledMessages,
      StateTransitionThrottleConfig.RebalanceType rebalanceType
      ) {
    boolean isRecovery = rebalanceType == StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE;
    if (isRecovery && onlyDownwardLoadBalance) {
      logger.error("onlyDownwardLoadBalance can't be used together with recovery_rebalance");
      return;
    }

    String resourceName = resource.getResourceName();

    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    messages.sort(new MessageThrottleComparator(bestPossibleMap, currentStateMap, messagePartitionMap, stateModelDef,isRecovery));
    logger.trace("throttleControllerstate->{} before load", throttleController);
    for (Message msg: messages) {
      if (onlyDownwardLoadBalance) {
        boolean isDownward = isDownwardTransition(idealState, cache, msg);
        if (isDownward == false) {
          throttledMessages.add(msg);
          if (logger.isDebugEnabled()) {
            LogUtil.logDebug(logger, _eventId,
                String.format("Message: %s throttled in resource as not downward: %s with type: %s", msg, resourceName,
                    rebalanceType));
          }
          continue;
        }
      }

      if (throttleController.shouldThrottleForResource(rebalanceType, resourceName)) {
        throttledMessages.add(msg);
        if (logger.isDebugEnabled()) {
          LogUtil.logDebug(logger, _eventId,
              String.format("Message: %s throttled in resource: %s with type: %s", msg, resourceName,
                  rebalanceType));
        }
        continue;
      }
      String instance = msg.getTgtName();
      if (throttleController.shouldThrottleForInstance(rebalanceType,instance)) {
        throttledMessages.add(msg);
        if (logger.isDebugEnabled()) {
          LogUtil.logDebug(logger, _eventId,
              String.format("Message: %s throttled in instance %s in resource: %s with type: %s", instance, msg, resourceName,
                  rebalanceType));
        }
        continue;
      }
      throttleController.chargeInstance(rebalanceType, instance);
      throttleController.chargeResource(rebalanceType, resourceName);
      throttleController.chargeCluster(rebalanceType);
      logger.trace("throttleControllerstate->{} after charge load msg: {}", throttleController, msg);
    }
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

  // compare message for throttling, note, all these message are of type state_transition: how about upward, downward?
  // recovery are all upward
  // 1) toState priority (toTop is higher than toSecond)
  // 2) same toState, the message classification time, the less required toState meeting minActive requirement has higher priority
  // 3) Higher priority for the partition of messages with fewer replicas with states matching with bestPossible ??? do we need this one
  private static class MessageThrottleComparator implements Comparator<Message> {
    private Map<Partition, Map<String, String>> _bestPossibleMap;
    private Map<Partition, Map<String, String>> _currentStateMap;
    private Map<Message, Partition> _messagePartitionMap;
    StateModelDefinition _stateModelDef;
    private boolean _recoveryRebalance;

    MessageThrottleComparator(Map<Partition, Map<String, String>> bestPossibleMap,
        Map<Partition, Map<String, String>> currentStateMap,
        Map<Message, Partition> messagePartitionMap, StateModelDefinition stateModelDef,
        boolean recoveryRebalance) {
      _bestPossibleMap = bestPossibleMap;
      _currentStateMap = currentStateMap;
      _recoveryRebalance = recoveryRebalance;
      _messagePartitionMap = messagePartitionMap;
      _stateModelDef = stateModelDef;
    }

    @Override
    public int compare(Message o1, Message o2) {
      if (_recoveryRebalance) {
        Map<String, Integer> statePriorityMap = _stateModelDef.getStatePriorityMap();
        Integer toStateP1 = statePriorityMap.get(o1.getToState());
        Integer toStateP2 = statePriorityMap.get(o2.getToState());
        // higher priority for topState
        if (!toStateP1.equals(toStateP2)) {
          return toStateP1.compareTo(toStateP2);
        }
      }
      Partition p1 = _messagePartitionMap.get(o1);
      Partition p2 = _messagePartitionMap.get(o2);
      // Higher priority for the partition with fewer active replicas
      int currentActiveReplicas1 = getCurrentActiveReplicas(p1);
      int currentActiveReplicas2 = getCurrentActiveReplicas(p2);
      if (currentActiveReplicas1 != currentActiveReplicas2) {
        return Integer.compare(currentActiveReplicas1, currentActiveReplicas2);
      }
      // Higher priority for the partition with fewer replicas with states matching with IdealState
      int idealStateMatched1 = getIdealStateMatched(p1);
      int idealStateMatched2 = getIdealStateMatched(p2);
      if (idealStateMatched1 != idealStateMatched2) {
        return Integer.compare(idealStateMatched1, idealStateMatched2);
      }
      // finally lexical order of partiton name + replica name
      String name1 = o1.getPartitionName() + o1.getTgtName();
      String name2 = o2.getPartitionName() + o2.getTgtName();
      return name1.compareTo(name2);
    }

    private int getIdealStateMatched(Partition partition) {
      int matchedState = 0;
      if (!_currentStateMap.containsKey(partition)) {
        return matchedState;
      }
      for (String instance : _bestPossibleMap.get(partition).keySet()) {
        if (_bestPossibleMap.get(partition).get(instance)
            .equals(_currentStateMap.get(partition).get(instance))) {
          matchedState++;
        }
      }
      return matchedState;
    }

    private int getCurrentActiveReplicas(Partition partition) {
      int currentActiveReplicas = 0;
      if (!_currentStateMap.containsKey(partition)) {
        return currentActiveReplicas;
      }
      // Initialize state -> number of this state map
      Map<String, Integer> stateCountMap = new HashMap<>();
      for (String state : _bestPossibleMap.get(partition).values()) {
        if (!stateCountMap.containsKey(state)) {
          stateCountMap.put(state, 0);
        }
        stateCountMap.put(state, stateCountMap.get(state) + 1);
      }
      // Search the state map
      for (String state : _currentStateMap.get(partition).values()) {
        if (stateCountMap.containsKey(state) && stateCountMap.get(state) > 0) {
          currentActiveReplicas++;
          stateCountMap.put(state, stateCountMap.get(state) - 1);
        }
      }
      return currentActiveReplicas;
    }
  }

  private static class PartitionMessageComparator implements Comparator<Message> {
    private StateModelDefinition _stateModelDef;

    PartitionMessageComparator(StateModelDefinition stateModelDef) {
      _stateModelDef = stateModelDef;
    }

    @Override
    public int compare(Message o1, Message o2) {

      Map<String, Integer> stateTransitionPriorities = getStateTransitionPriorityMap(_stateModelDef);

      Integer priority1 = Integer.MAX_VALUE;
      if (Message.MessageType.STATE_TRANSITION.name().equals(o1.getMsgType())) {
        String fromState1 = o1.getFromState();
        String toState1 = o1.getToState();
        String transition1 = fromState1 + "-" + toState1;

        if (stateTransitionPriorities.containsKey(transition1)) {
          priority1 = stateTransitionPriorities.get(transition1);
        }
      }

      Integer priority2 = Integer.MAX_VALUE;
      if (Message.MessageType.STATE_TRANSITION.name().equals(o1.getMsgType())) {
        String fromState2 = o1.getFromState();
        String toState2 = o1.getToState();
        String transition2 = fromState2 + "-" + toState2;

        if (stateTransitionPriorities.containsKey(transition2)) {
          priority2 = stateTransitionPriorities.get(transition2);
        }
      }

      if (!priority1.equals(priority2)) {
        return priority1.compareTo(priority2);
      }

      Long p1 = o1.getCreateTimeStamp();
      Long p2 = o2.getCreateTimeStamp();
      return p1.compareTo(p2);
    }

    private Map<String, Integer> getStateTransitionPriorityMap(StateModelDefinition stateModelDef) {
      Map<String, Integer> stateTransitionPriorities = new HashMap<String, Integer>();
      List<String> stateTransitionPriorityList = stateModelDef.getStateTransitionPriorityList();
      for (int i = 0; i < stateTransitionPriorityList.size(); i++) {
        stateTransitionPriorities.put(stateTransitionPriorityList.get(i), i);
      }

      return stateTransitionPriorities;
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
