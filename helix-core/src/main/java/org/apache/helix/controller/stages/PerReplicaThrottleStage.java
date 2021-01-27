package org.apache.helix.controller.stages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.ResourceMonitor;
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

    event.addAttribute(AttributeName.PER_REPLICA_OUTPUT_MESSAGES.name(), output);
    LogUtil.logDebug(logger,_eventId, String.format("retraceResourceStateMap is: %s", retracedResourceStateMap));
    event.addAttribute(AttributeName.PER_REPLICA_RETRACED_STATES.name(), retracedResourceStateMap);

    if (isEmitThrottledMsg) {
      event.addAttribute(AttributeName.PER_REPLICA_THROTTLED_RECOVERY_MESSAGES.name(), throttledRecoveryMsg);
      event.addAttribute(AttributeName.PER_REPLICA_THROTTLED_LOAD_MESSAGES.name(), throttledLoadMsg);
    }

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
      ResourcesStateMap retracedResourceStateMap,
      List<Message> throttledRecoveryMsg, List<Message> throttledLoadMsg) {
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
            throttledRecoveryMsg, throttledLoadMsg, output);
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

  private void propagateCountsTopDown(
      StateModelDefinition stateModelDef,
      Map<String, Integer> expectedStateCountMap
  ) {
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
      expectedStateCountMap.put(curState, prevCnt + expectedStateCountMap.getOrDefault(curState,0));
      prevState = curState;
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
    if (preferenceList == null) {
      preferenceList = Collections.emptyList();
    }

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
    propagateCountsTopDown(stateModelDef, expectedStateCountMapOut);
    propagateCountsTopDown(stateModelDef, currentStateCountsOut);
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
      Map<Partition, Map<String, String>> retracedPartitionsStateMap,
      Map<Partition, Map<String, Integer>> expectedStateCountByPartition,
      Map<Partition, Map<String, Integer>> currentStateCountsByPartition
  ) {
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

      // save these two maps for later classifying messages usage
      expectedStateCountByPartition.put(partition, expectedStateCountMap);
      currentStateCountsByPartition.put(partition, currentStateCounts);

      // TODO: the core logic in next PR
    }
  }

  /*
   * Classify the messages of each partition into recovery and load messages.
   */
  private void classifyMessages(Resource resource, CurrentStateOutput currentStateOutput,
      BestPossibleStateOutput bestPossibleStateOutput, IdealState idealState,
      ResourceControllerDataProvider cache, Map<Partition, List<Message>> selectedResourceMessages,
      List<Message> recoveryMessages, List<Message> loadMessages,
      Map<Message, Partition> messagePartitionMap,
      Map<Partition, Map<String, Integer>> expectedStateCountByPartition,
      Map<Partition, Map<String, Integer>> currentStateCountsByPartition) {

    String resourceName = resource.getResourceName();
    Map<String, List<String>> preferenceLists =
        bestPossibleStateOutput.getPreferenceLists(resourceName);
    LogUtil.logInfo(logger, _eventId, String.format("Classify message for resource: %s", resourceName));

    for (Partition partition : resource.getPartitions()) {
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

        if (isUpward && (currentCount < expectedCount)) {
          recoveryMessages.add(msg);
          currentStateCounts.put(toState, currentCount + 1);
        } else {
          loadMessages.add(msg);
        }
      }
    }
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
      List<Message> throttledRecoveryMsgOut, List<Message> throttledLoadMessageOut,
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

    Set<Partition> partitionsWithErrorStateReplica = new HashSet<>();
    Set<Partition> partitionsNeedRecovery = new HashSet<>();

    Map<Partition, Map<String, Integer>> expectedStateCountByPartition = new HashMap<>();
    Map<Partition, Map<String, Integer>> currentStateCountsByPartition = new HashMap<>();
    // Step 1: charge existing pending messages and update retraced state map.
    chargePendingMessages(resource, throttleController, currentStateOutput, bestPossibleStateOutput,
        idealState, cache, partitionsNeedRecovery, partitionsWithErrorStateReplica,
        retracedPartitionsStateMap, expectedStateCountByPartition, currentStateCountsByPartition);

    // Step 2: classify all the messages into recovery message list and load message list
    List<Message> recoveryMessages = new ArrayList<>();
    List<Message> loadMessages = new ArrayList<>();
    Map<Message, Partition> messagePartitionMap = new HashMap<>(); // todo: Message  may need a hashcode()
    classifyMessages(resource, currentStateOutput, bestPossibleStateOutput, idealState, cache,
        selectedResourceMessages, recoveryMessages, loadMessages, messagePartitionMap,
        expectedStateCountByPartition, currentStateCountsByPartition);

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
    // TODO: next PR
    // Step 5: construct output
    throttledRecoveryMsgOut.addAll(throttledRecoveryMessages);
    Map<Partition, List<Message>> outMessagesByPartition = new HashMap<>();
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
        // TODO: next PR load messages exclusion
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
        if (isDownwardTransition(idealState, cache, msg) == false) {
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

  // compare message for throttling, note, all these message are of type state_transition
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
