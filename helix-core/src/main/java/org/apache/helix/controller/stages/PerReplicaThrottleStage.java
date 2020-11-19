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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
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

    event.addAttribute(AttributeName.PER_REPLICA_THROTTLED_MESSAGES.name(), output);
    event.addAttribute(AttributeName.PER_REPLICA_RETRACED_STATES.name(), retracedResourceStateMap);

    // ToDo: handling maintenance maxPartitionPerInstance case.

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

    // Resource level prioritization based on the numerical (sortable) priority field.
    // If the resource priority field is null/not set, the resource will be treated as lowest
    // priority.
    List<ResourcePriority> prioritizedResourceList = new ArrayList<>();
    for (String resourceName : resourceMap.keySet()) {
      prioritizedResourceList.add(new ResourcePriority(resourceName, Integer.MIN_VALUE));
    }
    // If resourcePriorityField is null at the cluster level, all resources will be considered equal
    // in priority by keeping all priorities at MIN_VALUE
    if (dataCache.getClusterConfig().getResourcePriorityField() != null) {
      String priorityField = dataCache.getClusterConfig().getResourcePriorityField();
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
            computePerReplicaPartitionState(idealState, currentStateOutput,
                selectedMessage.getResourceMessages(resourceName), resourceMap.get(resourceName),
                bestPossibleStateOutput.getPreferenceLists(resourceName), dataCache,
                throttleController, retracedPartitionsState);
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
  private Map<Partition, List<Message>> computePerReplicaPartitionState(IdealState idealState,
      CurrentStateOutput currentStateOutput, Map<Partition, List<Message>> selectedResourceMessages,
      Resource resource, Map<String, List<String>> preferenceLists,
      ResourceControllerDataProvider cache, StateTransitionThrottleController throttleController,
      Map<Partition, Map<String, String>> retracedPartitionsStateMap ) {
    String resourceName = resource.getResourceName();
    LogUtil.logDebug(logger, _eventId, String.format("Processing resource: %s", resourceName));

    if (!throttleController.isThrottleEnabled() || !IdealState.RebalanceMode.FULL_AUTO
        .equals(idealState.getRebalanceMode())) {
      // todo: add retrace map?
      return selectedResourceMessages;
    }
    Set<Partition> partitionsWithErrorStateReplica = new HashSet<>();
    Set<Partition> partitionsNeedRecovery = new HashSet<>();

    // charge existing pending messages and update retraced state map.
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);
      Map<String, String> retracedStateMap = new HashMap<>(currentStateMap);

      if (currentStateMap.values().contains(HelixDefinedState.ERROR.name())) {
        partitionsWithErrorStateReplica.add(partition);
      }

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

      Map<String, Message> pendingMessageMap =
          currentStateOutput.getPendingMessageMap(resourceName, partition);
      List<Message> pendingMessages = new ArrayList<>(pendingMessageMap.values());
      // sort pendingMessages based on transition priority then timeStamp for state transition message
      pendingMessages.sort(new PartitionMessageComparator(stateModelDef));
      List<Message> recoveryMessages = new ArrayList<>();
      List<Message> loadMessages = new ArrayList<>();
      for (Message msg : pendingMessages) {
        if (!Message.MessageType.STATE_TRANSITION.name().equals(msg.getMsgType())) {
          // todo: log ignore pending messages
          // ignore cancellation message etc. For now, don't charge them.
          continue;
        }
        String toState = msg.getToState();
        if (toState.equals(HelixDefinedState.DROPPED.name()) || toState
            .equals(HelixDefinedState.ERROR.name())) {
          continue;
        }
        // todo: shall we confine this test to only upward transition?
        // todo: for upward n1 (s1->s2) then currentStateCount(s1)-- and currentStateCount(s1)++
        // todo: same for downward ?
        Integer expectedCount = expectedStateCountMap.get(toState);
        Integer currentCount = currentStateCounts.get(toState);
        expectedCount = expectedCount == null ? 0 : expectedCount;
        currentCount = currentCount == null ? 0 : currentCount;

        boolean isUpward = !isDownwardTransition(idealState, cache, msg);
        if (currentCount < expectedCount && isUpward) {
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
      }
      // charge load message and retrace
      for (Message loadMsg : loadMessages) {
        String toState = loadMsg.getToState();
        String toInstance = loadMsg.getTgtName();
        retracedStateMap.put(toInstance, toState);

        throttleController
            .chargeInstance(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE, toInstance);
        throttleController.chargeCluster(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE);
        throttleController
            .chargeResource(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE, resourceName);

        // todo: if  loadMsg is p2p message, charge relay S->M target with Recovery_BALANCE, but don't change retracedStateMap
      }
      retracedPartitionsStateMap.put(partition, retracedStateMap);
    }

    // classify all the messages as recovery message list and load message list
    List<Message> recoveryMessages = new ArrayList<>();
    List<Message> loadMessages = new ArrayList<>();
    // todo: shall we sort partition here?
    for (Partition partition : resource.getPartitions()) {
      // act as currentstate adjusted with pending message
      Map<String, String> retracedStateMap = retracedPartitionsStateMap.get(partition);

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
      Map<String, String> retracedStateMapWithoutDisabled = new HashMap<>(retracedStateMap);
      retracedStateMapWithoutDisabled.keySet().removeAll(cache
          .getDisabledInstancesForPartition(idealState.getResourceName(),
              partition.getPartitionName()));
      Map<String, Integer> retracedStateCounts =
          StateModelDefinition.getStateCounts(retracedStateMapWithoutDisabled);

      List<Message> partitionMessages = selectedResourceMessages.get(partition);
      if (partitionMessages == null) {
        continue;
      }
      // sort partitionMessages based on transition priority and then creation timestamp for transition message
      partitionMessages.sort(new PartitionMessageComparator(stateModelDef));
      for (Message msg : partitionMessages) {
        if (!Message.MessageType.STATE_TRANSITION.name().equals(msg.getMsgType())) {
          // todo: log ignore pending messages
          // ignore cancellation message etc. For now, don't charge them.
          continue;
        }
        String toState = msg.getToState();
        if (toState.equals(HelixDefinedState.DROPPED.name()) || toState
            .equals(HelixDefinedState.ERROR.name())) {
          continue;
        }
        Integer expectedCount = expectedStateCountMap.get(toState);
        Integer currentCount = retracedStateCounts.get(toState);
        expectedCount = expectedCount == null ? 0 : expectedCount;
        currentCount = currentCount == null ? 0 : currentCount;

        if (currentCount < expectedCount) {
          recoveryMessages.add(msg);
        } else {
          loadMessages.add(msg);
          // todo: p2p handling
        }
        // update
        retracedStateCounts.put(toState, currentCount + 1);
      }
    }

    // sorting recovery message list and apply throttling
    // todo: sort recovery messages
    Set<Message> throttledRecoveryMessages = new HashSet<>();
    //recoveryMessages.sort(new );
    for (Message msg : recoveryMessages) {
      // step 1. if the instance level throttle met, consider disabled instance case
      // step 2. if the resource level throttle met
      // step 3. if none of them met, charge
      StateTransitionThrottleConfig.RebalanceType rebalanceType = StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE;
      if (throttleController.shouldThrottleForResource(rebalanceType, resourceName)) {
        throttledRecoveryMessages.add(msg);
        if (logger.isDebugEnabled()) {
          LogUtil.logDebug(logger, _eventId,
              String.format("Message: %s throttled in resource: %s with type: %s", msg, resourceName,
                  rebalanceType));
        }
        continue;
      }
      String instance = msg.getTgtName();
      if (!cache.getDisabledInstances().contains(instance)) {
        throttledRecoveryMessages.add(msg);
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
    }

    // sorting load message list and apply throttling

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
    // todo: sort load messages
    Set<Message> throttledLoadMessages = new HashSet<>();
    for (Message msg : loadMessages) {
      StateTransitionThrottleConfig.RebalanceType rebalanceType = StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE;
      if (onlyDownwardLoadBalance) {
        boolean isDownward = isDownwardTransition(idealState, cache, msg);
        if (isDownward == false) {
          throttledLoadMessages.add(msg);
          if (logger.isDebugEnabled()) {
            LogUtil.logDebug(logger, _eventId,
                String.format("Message: %s throttled in resource as not downward: %s with type: %s", msg, resourceName,
                    rebalanceType));
          }
          continue;
        }
      }
      if (throttleController.shouldThrottleForResource(rebalanceType, resourceName)) {
        throttledLoadMessages.add(msg);
        if (logger.isDebugEnabled()) {
          LogUtil.logDebug(logger, _eventId,
              String.format("Message: %s throttled in resource: %s with type: %s", msg, resourceName,
                  rebalanceType));
        }
        continue;
      }
      String instance = msg.getTgtName();
      if (!cache.getDisabledInstances().contains(instance)) {
        throttledLoadMessages.add(msg);
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
    }

    // construct output and retraced state
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

    // construct all retraced partition state map for the resource
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
    return out;
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
  // 3) Higher priority for the partition with fewer replicas with states matching with IdealState ??? do we need this one
  private static class MessageThrottleComparator implements Comparator<Message> {

    MessageThrottleComparator() {

    }

    @Override
    public int compare(Message o1, Message o2) {
      return 0;
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
