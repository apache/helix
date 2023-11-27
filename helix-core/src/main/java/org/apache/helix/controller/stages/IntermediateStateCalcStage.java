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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.api.config.StateTransitionThrottleConfig.RebalanceType;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.common.PartitionStateMap;
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
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.ResourceMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For partition compute the Intermediate State (instance,state) pair based on the BestPossibleState
 * and CurrentState, with all constraints applied (such as state transition throttling).
 */
public class IntermediateStateCalcStage extends AbstractBaseStage {
  private static final Logger logger =
      LoggerFactory.getLogger(IntermediateStateCalcStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    CurrentStateOutput currentStateOutput = event.getAttribute(AttributeName.CURRENT_STATE.name());
    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    Map<String, Resource> resourceToRebalance =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    ResourceControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    MessageOutput messageOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());

    if (currentStateOutput == null || bestPossibleStateOutput == null || resourceToRebalance == null
        || cache == null || messageOutput == null) {
      throw new StageException(String.format("Missing attributes in event: %s. "
              + "Requires CURRENT_STATE (%s) |BEST_POSSIBLE_STATE (%s) |RESOURCES (%s) |MESSAGE_SELECT (%s) |DataCache (%s)",
          event, currentStateOutput, bestPossibleStateOutput, resourceToRebalance, messageOutput,
          cache));
    }

    IntermediateStateOutput intermediateStateOutput =
        compute(event, resourceToRebalance, currentStateOutput, bestPossibleStateOutput,
            messageOutput);
    event.addAttribute(AttributeName.INTERMEDIATE_STATE.name(), intermediateStateOutput);

    // Make sure no instance has more replicas/partitions assigned than maxPartitionPerInstance. If
    // it does, pause the rebalance and put the cluster on maintenance mode
    int maxPartitionPerInstance = cache.getClusterConfig().getMaxPartitionsPerInstance();
    if (maxPartitionPerInstance > 0) {
      validateMaxPartitionsPerInstance(event, cache, intermediateStateOutput,
          maxPartitionPerInstance);
    }
  }

  /**
   * Go through each resource, and based on BestPossibleState and CurrentState, compute
   * IntermediateState as close to BestPossibleState while maintaining throttling constraints (for
   * example, ensure that the number of possible pending state transitions does NOT go over the set
   * threshold).
   * @param event
   * @param resourceMap
   * @param currentStateOutput
   * @param bestPossibleStateOutput
   * @return
   */
  private IntermediateStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleStateOutput,
      MessageOutput messageOutput) {
    IntermediateStateOutput output = new IntermediateStateOutput();
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
      prioritizedResourceList.add(new ResourcePriority(resourceName));
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
      Collections.sort(prioritizedResourceList);
    }

    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    List<String> failedResources = new ArrayList<>();

    // Priority is applied in assignment computation because higher priority by looping in order of
    // decreasing priority
    for (ResourcePriority resourcePriority : prioritizedResourceList) {
      String resourceName = resourcePriority.getResourceName();

      if (!bestPossibleStateOutput.containsResource(resourceName)) {
        LogUtil.logInfo(logger, _eventId, String.format(
            "Skip calculating intermediate state for resource %s because the best possible state is not available.",
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

      try {
        output.setState(resourceName,
            computeIntermediatePartitionState(dataCache, clusterStatusMonitor, idealState,
                resourceMap.get(resourceName), currentStateOutput,
                bestPossibleStateOutput.getPartitionStateMap(resourceName),
                bestPossibleStateOutput.getPreferenceLists(resourceName), throttleController,
                messageOutput.getResourceMessageMap(resourceName)));
      } catch (HelixException ex) {
        LogUtil.logInfo(logger, _eventId,
            "Failed to calculate intermediate partition states for resource " + resourceName, ex);
        failedResources.add(resourceName);
      }
    }

    if (clusterStatusMonitor != null) {
      clusterStatusMonitor.setResourceRebalanceStates(failedResources,
          ResourceMonitor.RebalanceStatus.INTERMEDIATE_STATE_CAL_FAILED);
      clusterStatusMonitor
          .setResourceRebalanceStates(output.resourceSet(), ResourceMonitor.RebalanceStatus.NORMAL);
    }

    return output;
  }

  /**
   * Go through every instance in the assignment and check that each instance does NOT have more
   * replicas for partitions assigned to it than maxPartitionsPerInstance. If the assignment
   * violates this, put the cluster on maintenance mode.
   * This logic could be integrated with compute() for IntermediateState calculation but appended
   * separately for visibility and testing. Additionally, performing validation after compute()
   * ensures that we have a full intermediate state mapping complete prior to validation.
   * @param event
   * @param cache
   * @param intermediateStateOutput
   * @param maxPartitionPerInstance
   */
  private void validateMaxPartitionsPerInstance(ClusterEvent event,
      ResourceControllerDataProvider cache, IntermediateStateOutput intermediateStateOutput,
      int maxPartitionPerInstance) {
    Map<String, PartitionStateMap> resourceStatesMap =
        intermediateStateOutput.getResourceStatesMap();
    Map<String, Integer> instancePartitionCounts = new HashMap<>();

    for (String resource : resourceStatesMap.keySet()) {
      IdealState idealState = cache.getIdealState(resource);
      if (idealState != null && idealState.getStateModelDefRef()
          .equals(BuiltInStateModelDefinitions.Task.name())) {
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
          int partitionCount = instancePartitionCounts.get(instance); // Number of replicas (from
          // different partitions) held
          // in this instance
          partitionCount++;
          if (partitionCount > maxPartitionPerInstance) {
            HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
            String errMsg = String.format(
                "Problem: according to this assignment, instance %s contains more "
                    + "replicas/partitions than the maximum number allowed (%d). Pipeline will "
                    + "stop the rebalance and put the cluster %s into maintenance mode", instance,
                maxPartitionPerInstance, cache.getClusterName());
            if (manager != null) {
              if (manager.getHelixDataAccessor()
                  .getProperty(manager.getHelixDataAccessor().keyBuilder().maintenance()) == null) {
                manager.getClusterManagmentTool()
                    .autoEnableMaintenanceMode(manager.getClusterName(), true, errMsg,
                        MaintenanceSignal.AutoTriggerReason.MAX_PARTITION_PER_INSTANCE_EXCEEDED);
              }
              LogUtil.logWarn(logger, _eventId, errMsg);
            } else {
              LogUtil.logError(logger, _eventId,
                  "HelixManager is not set/null! Failed to pause this cluster/enable maintenance"
                      + " mode due to an instance being assigned more replicas/partitions than "
                      + "the limit.");
            }

            ClusterStatusMonitor clusterStatusMonitor =
                event.getAttribute(AttributeName.clusterStatusMonitor.name());
            if (clusterStatusMonitor != null) {
              clusterStatusMonitor.setResourceRebalanceStates(Collections.singletonList(resource),
                  ResourceMonitor.RebalanceStatus.INTERMEDIATE_STATE_CAL_FAILED);
            }
            // Throw an exception here so that messages won't be sent out based on this mapping
            throw new HelixException(errMsg);
          }
          instancePartitionCounts.put(instance, partitionCount);
        }
      }
    }
  }

  /**
   * Compute intermediate partition states for a prioritized resource.
   * @param cache
   * @param clusterStatusMonitor
   * @param idealState
   * @param resource
   * @param currentStateOutput
   * @param bestPossiblePartitionStateMap
   * @param preferenceLists
   * @param throttleController
   * @return
   */
  private PartitionStateMap computeIntermediatePartitionState(ResourceControllerDataProvider cache,
      ClusterStatusMonitor clusterStatusMonitor, IdealState idealState, Resource resource,
      CurrentStateOutput currentStateOutput, PartitionStateMap bestPossiblePartitionStateMap,
      Map<String, List<String>> preferenceLists,
      StateTransitionThrottleController throttleController,
      Map<Partition, List<Message>> resourceMessageMap) {
    String resourceName = resource.getResourceName();
    LogUtil.logDebug(logger, _eventId, String.format("Processing resource: %s", resourceName));

    // Throttling is applied only on FULL-AUTO mode and if the resource message map is empty, no throttling needed.
    // TODO: The potential optimization to make the logic computation async and report the metric for recovery/load
    // rebalance.
    if (!IdealState.RebalanceMode.FULL_AUTO.equals(idealState.getRebalanceMode())
        || resourceMessageMap.isEmpty()) {
      return bestPossiblePartitionStateMap;
    }

    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    // This requires a deep copy of current state map because some of the states will be overwritten by applying
    // messages to it.

    Set<Partition> partitionsWithErrorStateReplica = new HashSet<>();
    Set<String> messagesForRecovery = new HashSet<>();
    Set<String> messagesForLoad = new HashSet<>();
    Set<String> messagesThrottledForRecovery = new HashSet<>();
    Set<String> messagesThrottledForLoad = new HashSet<>();
    ClusterConfig clusterConfig = cache.getClusterConfig();

    // If the threshold (ErrorOrRecovery) is set, then use it, if not, then check if the old
    // threshold (Error) is set. If the old threshold is set, use it. If not, use the default value
    // for the new one. This is for backward-compatibility
    int threshold = 1; // Default threshold for ErrorOrRecoveryPartitionThresholdForLoadBalance
    // Keep the error count as partition level. This logic only applies to downward state transition determination
    for (Partition partition : currentStateOutput.getCurrentStateMap(resourceName).keySet()) {
      Map<String, String> entry =
          currentStateOutput.getCurrentStateMap(resourceName).get(partition);
      if (entry.values().stream().anyMatch(x -> x.contains(HelixDefinedState.ERROR.name()))) {
        partitionsWithErrorStateReplica.add(partition);
      }
    }
    int numPartitionsWithErrorReplica = partitionsWithErrorStateReplica.size();
    if (clusterConfig.getErrorOrRecoveryPartitionThresholdForLoadBalance() != -1) {
      // ErrorOrRecovery is set
      threshold = clusterConfig.getErrorOrRecoveryPartitionThresholdForLoadBalance();
    } else {
      if (clusterConfig.getErrorPartitionThresholdForLoadBalance() != 0) {
        // 0 is the default value so the old threshold has been set
        threshold = clusterConfig.getErrorPartitionThresholdForLoadBalance();
      }
    }

    // Perform regular load balance only if the number of partitions in recovery and in error is
    // less than the threshold. Otherwise, only allow downward-transition load balance
    boolean onlyDownwardLoadBalance = numPartitionsWithErrorReplica > threshold;

    chargePendingTransition(resource, currentStateOutput, throttleController, cache,
        preferenceLists, stateModelDef);

    // Sort partitions in case of urgent partition need to take the quota first.
    List<Partition> partitions = new ArrayList<>(resource.getPartitions());
    partitions.sort(new PartitionPriorityComparator(bestPossiblePartitionStateMap.getStateMap(),
        currentStateOutput.getCurrentStateMap(resourceName), stateModelDef.getTopState()));
    for (Partition partition : partitions) {
      if (resourceMessageMap.get(partition) == null || resourceMessageMap.get(partition)
          .isEmpty()) {
        continue;
      }
      List<Message> messagesToThrottle = new ArrayList<>(resourceMessageMap.get(partition));
      Map<String, String> derivedCurrentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition).entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
      Map<String, Integer> requiredState = getRequiredStates(resourceName, cache, preferenceList);
      if (preferenceList != null && !preferenceList.isEmpty()) {
        // Sort messages based on the priority (priority is defined in the state model definition
        messagesToThrottle.sort(new MessagePriorityComparator(preferenceList, stateModelDef.getStatePriorityMap()));
      }
      for (Message message : messagesToThrottle) {
        RebalanceType rebalanceType =
            getRebalanceTypePerMessage(requiredState, message, derivedCurrentStateMap);

        // Number of states required by StateModelDefinition are not satisfied, need recovery
        if (rebalanceType.equals(RebalanceType.RECOVERY_BALANCE)) {
          message.setSTRebalanceType(Message.STRebalanceType.RECOVERY_REBALANCE);
          messagesForRecovery.add(message.getId());
          recoveryRebalance(resource, partition, throttleController, message, cache,
              messagesThrottledForRecovery, resourceMessageMap);
        } else if (rebalanceType.equals(RebalanceType.LOAD_BALANCE)) {
          message.setSTRebalanceType(Message.STRebalanceType.LOAD_REBALANCE);
          messagesForLoad.add(message.getId());
          loadRebalance(resource, partition, throttleController, message, cache,
              onlyDownwardLoadBalance, stateModelDef, messagesThrottledForLoad, resourceMessageMap);
        }

        // Apply the message to temporary current state map
        if (!messagesThrottledForRecovery.contains(message.getId()) && !messagesThrottledForLoad
            .contains(message.getId())) {
          derivedCurrentStateMap.put(message.getTgtName(), message.getToState());
        }
      }
    }
    // TODO: We may need to optimize it to be async compute for intermediate state output.
    PartitionStateMap intermediatePartitionStateMap =
        new PartitionStateMap(resourceName, currentStateOutput.getCurrentStateMap(resourceName));
    computeIntermediateMap(intermediatePartitionStateMap,
        currentStateOutput.getPendingMessageMap(resourceName), resourceMessageMap);

    if (!messagesForRecovery.isEmpty()) {
      LogUtil.logInfo(logger, _eventId, String
          .format("Recovery balance needed for %s with messages: %s", resourceName,
              messagesForRecovery));
    }
    if (!messagesForLoad.isEmpty()) {
      LogUtil.logInfo(logger, _eventId, String
          .format("Load balance needed for %s with messages: %s", resourceName, messagesForLoad));
    }
    if (!partitionsWithErrorStateReplica.isEmpty()) {
      LogUtil.logInfo(logger, _eventId, String
          .format("Partition currently has an ERROR replica in %s partitions: %s", resourceName,
              partitionsWithErrorStateReplica));
    }

    if (clusterStatusMonitor != null) {
      clusterStatusMonitor
          .updateRebalancerStats(resourceName, messagesForRecovery.size(), messagesForLoad.size(),
              messagesThrottledForRecovery.size(), messagesThrottledForLoad.size(),
              onlyDownwardLoadBalance);
    }

    if (logger.isDebugEnabled()) {
      logPartitionMapState(resourceName, new HashSet<>(resource.getPartitions()),
          messagesForRecovery, messagesThrottledForRecovery, messagesForLoad,
          messagesThrottledForLoad, currentStateOutput, bestPossiblePartitionStateMap,
          intermediatePartitionStateMap);
    }

    LogUtil.logDebug(logger, _eventId, String.format("End processing resource: %s", resourceName));
    return intermediatePartitionStateMap;
  }

  /**
   * Determine the message is downward message or not.
   * @param message                  message for load rebalance
   * @param stateModelDefinition     state model definition object for this resource
   * @return set of messages allowed for downward state transitions
   */
  private boolean isLoadBalanceDownwardStateTransition(Message message,
      StateModelDefinition stateModelDefinition) {
    // state model definition is not found
    if (stateModelDefinition == null) {
      return false;
    }

    Map<String, Integer> statePriorityMap = stateModelDefinition.getStatePriorityMap();
    // Compare priority values and return if an upward transition is found
    // Note that lower integer value implies higher priority
    // If the state is not found in statePriorityMap, consider it not strictly downward by
    // default because we can't determine whether it is downward
    return statePriorityMap.containsKey(message.getFromState())
        && statePriorityMap.containsKey(message.getToState())
        && statePriorityMap.get(message.getFromState()) < statePriorityMap.get(message.getToState());
  }

  /**
   * Check and charge all pending transitions for throttling.
   */
  private void chargePendingTransition(Resource resource, CurrentStateOutput currentStateOutput,
      StateTransitionThrottleController throttleController, ResourceControllerDataProvider cache,
      Map<String, List<String>> preferenceLists, StateModelDefinition stateModelDefinition) {
    String resourceName = resource.getResourceName();
    // check and charge pending transitions
    for (Partition partition : resource.getPartitions()) {
      // To clarify that custom mode does not apply recovery/load rebalance since user can define different number of
      // replicas for different partitions. Actually, the custom will stopped from resource level checks if this resource
      // is not FULL_AUTO, we will return best possible state and do nothing.
      Map<String, Integer> requiredStates =
          getRequiredStates(resourceName, cache, preferenceLists.get(partition.getPartitionName()));
      // Maps instance to its current state
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);
      // Maps instance to its pending (next) state
      List<Message> pendingMessages = new ArrayList<>(
          currentStateOutput.getPendingMessageMap(resourceName, partition).values());
      List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
      if (preferenceList != null && !preferenceList.isEmpty()) {
        pendingMessages.sort(new MessagePriorityComparator(preferenceList,
            stateModelDefinition.getStatePriorityMap()));
      }

      for (Message message : pendingMessages) {
        StateTransitionThrottleConfig.RebalanceType rebalanceType =
            getRebalanceTypePerMessage(requiredStates, message, currentStateMap);
        String currentState = currentStateMap.get(message.getTgtName());
        if (currentState == null) {
          currentState = stateModelDefinition.getInitialState();
        }
        if (!message.getToState().equals(currentState) && message.getFromState()
            .equals(currentState) && !cache
            .getDisabledInstancesForPartition(resourceName, partition.getPartitionName())
            .contains(message.getTgtName())) {
          throttleController.chargeInstance(rebalanceType, message.getTgtName());
          throttleController.chargeResource(rebalanceType, resourceName);
          throttleController.chargeCluster(rebalanceType);
        }
      }
    }
  }

  /**
   * Thin wrapper for per message throttling with recovery rebalance type. Also populate
   * intermediatePartitionStateMap with generated messages from {@link MessageGenerationPhase}.
   * @param resource                      the resource to throttle
   * @param throttleController            throttle controller object
   * @param messageToThrottle             the message to be throttled
   * @param cache                         cache object for computational metadata from external storage
   * @param messagesThrottled             messages that have already been throttled
   * @param resourceMessageMap            the map for all messages from MessageSelectStage. Remove the message
   *                                      if it has been throttled
   */
  private void recoveryRebalance(Resource resource, Partition partition,
      StateTransitionThrottleController throttleController, Message messageToThrottle,
      ResourceControllerDataProvider cache, Set<String> messagesThrottled,
      Map<Partition, List<Message>> resourceMessageMap) {
    throttleStateTransitionsForReplica(throttleController, resource.getResourceName(), partition,
        messageToThrottle, messagesThrottled, RebalanceType.RECOVERY_BALANCE, cache,
        resourceMessageMap);
  }

  /**
   * Thin wrapper for per message throttling with load rebalance type. Also populate
   * intermediatePartitionStateMap with generated messages from {@link MessageGenerationPhase}.
   * @param resource                      the resource to throttle
   * @param throttleController            throttle controller object
   * @param messageToThrottle             the message to be throttle
   * @param cache                         cache object for computational metadata from external storage
   * @param onlyDownwardLoadBalance       does allow only downward load balance
   * @param stateModelDefinition          state model definition of this resource
   * @param messagesThrottled             messages are already throttled
   * @param resourceMessageMap            the map for all messages from MessageSelectStage. Remove the message
   *                                      if it has been throttled
   */
  private void loadRebalance(Resource resource, Partition partition,
      StateTransitionThrottleController throttleController, Message messageToThrottle,
      ResourceControllerDataProvider cache, boolean onlyDownwardLoadBalance,
      StateModelDefinition stateModelDefinition, Set<String> messagesThrottled,
      Map<Partition, List<Message>> resourceMessageMap) {
    // TODO: refactor the logic into throttling to let throttling logic to handle only downward including recovery rebalance
    // If only downward allowed: 1) any non-downward ST messages will be throttled and removed.
    //                           2) any downward ST messages will respect the throttling.
    // If not only downward allowed, all ST messages should respect the throttling.
    if (onlyDownwardLoadBalance && !isLoadBalanceDownwardStateTransition(messageToThrottle,
        stateModelDefinition)) {
      resourceMessageMap.get(partition).remove(messageToThrottle);
      messagesThrottled.add(messageToThrottle.getId());
      return;
    }
    // TODO: Currently throttling is applied for messages that are targeting all instances including those not considered as
    //  assignable. They all share the same configured limits. After discussion, there was agreement that this is the proper
    //  behavior. In addition to this, we should consider adding priority based on whether the instance is assignable and whether
    //  the message is bringing replica count to configured replicas or above configured replicas.
    throttleStateTransitionsForReplica(throttleController, resource.getResourceName(), partition,
        messageToThrottle, messagesThrottled, RebalanceType.LOAD_BALANCE, cache,
        resourceMessageMap);
  }

  /**
   * Check the status for a single message on throttling at every level (cluster, resource, replica) and set
   * intermediatePartitionStateMap accordingly for that replica.
   * @param throttleController                throttle controller object for throttling quota
   * @param resourceName                      the resource for throttling check
   * @param partition                         the partition for throttling check
   * @param messageToThrottle                 the message to be throttled
   * @param messagesThrottled                 the cumulative set of messages that have been throttled already. These
   *                                          messages represent the replicas of this partition that have been throttled.
   * @param rebalanceType                     the rebalance type to charge quota
   * @param cache                             cached cluster metadata required by the throttle controller
   * @param resourceMessageMap                the map for all messages from MessageSelectStage. Remove the message
   *                                          if it has been throttled.
   */
  private void throttleStateTransitionsForReplica(
      StateTransitionThrottleController throttleController, String resourceName,
      Partition partition, Message messageToThrottle, Set<String> messagesThrottled,
      RebalanceType rebalanceType, ResourceControllerDataProvider cache,
      Map<Partition, List<Message>> resourceMessageMap) {
    boolean hasReachedThrottlingLimit = false;
    if (throttleController.shouldThrottleForResource(rebalanceType, resourceName)) {
      hasReachedThrottlingLimit = true;
      if (logger.isDebugEnabled()) {
        LogUtil.logDebug(logger, _eventId, String.format(
            "Throttled because of cluster/resource quota is full for message {%s} on partition {%s} in resource {%s}",
            messageToThrottle.getId(), partition.getPartitionName(), resourceName));
      }
    } else {
      // Since message already generated, we can assume the current state is not null and target state is not null
      if (!cache.getDisabledInstancesForPartition(resourceName, partition.getPartitionName())
          .contains(messageToThrottle.getTgtName())) {
        if (throttleController
            .shouldThrottleForInstance(rebalanceType, messageToThrottle.getTgtName())) {
          hasReachedThrottlingLimit = true;
          if (logger.isDebugEnabled()) {
            LogUtil.logDebug(logger, _eventId, String.format(
                "Throttled because of instance level quota is full on instance {%s} for message {%s} of partition {%s} in resource {%s}",
                messageToThrottle.getId(), messageToThrottle.getTgtName(),
                partition.getPartitionName(), resourceName));
          }
        }
      }
    }
    // If there is still room for this replica, proceed to charge at the cluster and resource level and set the
    // intermediate partition-state mapping so that the state transition message can move forward.
    if (!hasReachedThrottlingLimit) {
      throttleController.chargeCluster(rebalanceType);
      throttleController.chargeResource(rebalanceType, resourceName);
      throttleController.chargeInstance(rebalanceType, messageToThrottle.getTgtName());
    } else {
      // Intermediate Map is based on current state
      // Remove the message from MessageSelection result if it has been throttled since the message will be dispatched
      // by next stage if it is not removed.
      resourceMessageMap.get(partition).remove(messageToThrottle);
      messagesThrottled.add(messageToThrottle.getId());
    }
  }

  /**
   * Determine the message rebalance type with message and current states.
   * @param desiredStates         Ideally how may states we needed for guarantee the health of replica
   * @param message               The message to be determined what is the rebalance type
   * @param derivedCurrentStates  Derived from current states with previous messages not be throttled.
   * @return Rebalance type. Recovery or load.
   */
  private RebalanceType getRebalanceTypePerMessage(Map<String, Integer> desiredStates,
      Message message, Map<String, String> derivedCurrentStates) {
    Map<String, Integer> desiredStatesSnapshot = new HashMap<>(desiredStates);
    // Looping existing current states to see whether current states fulfilled all the required states.
    for (String state : derivedCurrentStates.values()) {
      if (desiredStatesSnapshot.containsKey(state)) {
        if (desiredStatesSnapshot.get(state) == 1) {
          desiredStatesSnapshot.remove(state);
        } else {
          desiredStatesSnapshot.put(state, desiredStatesSnapshot.get(state) - 1);
        }
      }
    }

    // If the message contains any "required" state changes, then it is considered recovery rebalance.
    // Otherwise, it is load balance.
    return desiredStatesSnapshot.containsKey(message.getToState()) ? RebalanceType.RECOVERY_BALANCE
        : RebalanceType.LOAD_BALANCE;
  }

  private Map<String, Integer> getRequiredStates(String resourceName,
      ResourceControllerDataProvider resourceControllerDataProvider, List<String> preferenceList) {

    // Prepare required inputs: 1) Priority State List 2) required number of replica
    IdealState idealState = resourceControllerDataProvider.getIdealState(resourceName);
    StateModelDefinition stateModelDefinition =
        resourceControllerDataProvider.getStateModelDef(idealState.getStateModelDefRef());
    int requiredNumReplica =
        idealState.getMinActiveReplicas() == -1 ?
            idealState.getReplicaCount(preferenceList == null ? 0 : preferenceList.size())
            : idealState.getMinActiveReplicas();

    // Generate a state mapping, state -> required numbers based on the live and enabled instances for this partition
    // preference list
    if (preferenceList != null) {
      return stateModelDefinition.getStateCountMap((int) preferenceList.stream().filter(
              i -> resourceControllerDataProvider.getAssignableEnabledLiveInstances().contains(i))
          .count(), requiredNumReplica); // StateModelDefinition's counts
    }
    return stateModelDefinition.getStateCountMap(
        resourceControllerDataProvider.getAssignableEnabledLiveInstances().size(),
        requiredNumReplica); // StateModelDefinition's counts
  }

  /**
   * Log rebalancer metadata for debugging purposes.
   * @param resource
   * @param allPartitions
   * @param recoveryPartitions
   * @param recoveryThrottledPartitions
   * @param loadbalancePartitions
   * @param loadbalanceThrottledPartitions
   * @param currentStateOutput
   * @param bestPossibleStateMap
   * @param intermediateStateMap
   */
  private void logPartitionMapState(String resource, Set<Partition> allPartitions,
      Set<String> recoveryPartitions, Set<String> recoveryThrottledPartitions,
      Set<String> loadbalancePartitions, Set<String> loadbalanceThrottledPartitions,
      CurrentStateOutput currentStateOutput, PartitionStateMap bestPossibleStateMap,
      PartitionStateMap intermediateStateMap) {

    if (logger.isDebugEnabled()) {
      LogUtil.logDebug(logger, _eventId, String
          .format("Partitions need recovery: %s\nPartitions get throttled on recovery: %s",
              recoveryPartitions, recoveryThrottledPartitions));
      LogUtil.logDebug(logger, _eventId, String
          .format("Partitions need loadbalance: %s\nPartitions get throttled on load-balance: %s",
              loadbalancePartitions, loadbalanceThrottledPartitions));
    }

    for (Partition partition : allPartitions) {
      if (logger.isDebugEnabled()) {
        LogUtil.logDebug(logger, _eventId, String.format("%s : Best possible map: %s", partition,
            bestPossibleStateMap.getPartitionMap(partition)));
        LogUtil.logDebug(logger, _eventId, String.format("%s : Current State: %s", partition,
            currentStateOutput.getCurrentStateMap(resource, partition)));
        LogUtil.logDebug(logger, _eventId, String.format("%s: Pending state: %s", partition,
            currentStateOutput.getPendingMessageMap(resource, partition)));
        LogUtil.logDebug(logger, _eventId, String.format("%s: Intermediate state: %s", partition,
            intermediateStateMap.getPartitionMap(partition)));
      }
    }
  }

  /**
   * POJO that maps resource name to its priority represented by an integer.
   */
  private static class ResourcePriority implements Comparable<ResourcePriority> {
    private final String _resourceName;
    private int _priority = Integer.MIN_VALUE;

    ResourcePriority(String resourceName) {
      _resourceName = resourceName;
    }

    @Override
    public int compareTo(ResourcePriority resourcePriority) {
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

  private static class MessagePriorityComparator implements Comparator<Message> {
    private final Map<String, Integer> _preferenceInstanceMap;
    private final Map<String, Integer> _statePriorityMap;

    MessagePriorityComparator(List<String> preferenceList, Map<String, Integer> statePriorityMap) {
      // Get instance -> priority map.
      _preferenceInstanceMap = IntStream.range(0, preferenceList.size()).boxed()
          .collect(Collectors.toMap(preferenceList::get, index -> index));
      _statePriorityMap = statePriorityMap;
    }

    @Override
    public int compare(Message m1, Message m2) {
      //  Compare rules:
      //     1. Higher target state has higher priority.
      //     2. If target state is same, range it as preference list order.
      //     3. Sort by the name of targeted instances just for deterministic ordering.
      if (m1.getToState().equals(m2.getToState()) && _preferenceInstanceMap
          .containsKey(m1.getTgtName()) && _preferenceInstanceMap.containsKey(m2.getTgtName())) {
        return _preferenceInstanceMap.get(m1.getTgtName())
            .compareTo(_preferenceInstanceMap.get(m2.getTgtName()));
      }
      if (!m1.getToState().equals(m2.getToState())) {
        return _statePriorityMap.get(m1.getToState())
            .compareTo(_statePriorityMap.get(m2.getToState()));
      }
      return m1.getTgtName().compareTo(m2.getTgtName());
    }
  }

  // Compare partitions according following standard:
  // 1) Partition without top state always is the highest priority.
  // 2) For partition with top-state, the more number of active replica it has, the less priority.
  private static class PartitionPriorityComparator implements Comparator<Partition> {
    private final Map<Partition, Map<String, String>> _bestPossibleMap;
    private final Map<Partition, Map<String, String>> _currentStateMap;
    private final String _topState;

    PartitionPriorityComparator(Map<Partition, Map<String, String>> bestPossibleMap,
        Map<Partition, Map<String, String>> currentStateMap, String topState) {
      _bestPossibleMap = bestPossibleMap;
      _currentStateMap = currentStateMap;
      _topState = topState;
    }

    @Override
    public int compare(Partition p1, Partition p2) {
      int missTopState1 = getMissTopStateIndex(p1);
      int missTopState2 = getMissTopStateIndex(p2);
      // Highest priority for the partition without top state
      if (missTopState1 != missTopState2) {
        return Integer.compare(missTopState1, missTopState2);
      }
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
      return p1.getPartitionName().compareTo(p2.getPartitionName());
    }

    private int getMissTopStateIndex(Partition partition) {
      // 0 if no replicas in top-state, 1 if it has at least one replica in top-state.
      if (!_currentStateMap.containsKey(partition) || !_currentStateMap.get(partition).containsValue(_topState)) {
        return 0;
      }
      return 1;
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
  }

  /**
   * Generate the IntermediateStateMap from pending messages + message generated.
   */
  private void computeIntermediateMap(PartitionStateMap intermediateStateMap,
      Map<Partition, Map<String, Message>> pendingMessageMap,
      Map<Partition, List<Message>> resourceMessageMap) {
    for (Map.Entry<Partition, Map<String, Message>> entry : pendingMessageMap.entrySet()) {
      entry.getValue().forEach((key, value) -> {
        if (!value.getToState().equals(HelixDefinedState.DROPPED.name())) {
          intermediateStateMap.setState(entry.getKey(), value.getTgtName(), value.getToState());
        } else {
          intermediateStateMap.getStateMap().get(entry.getKey()).remove(value.getTgtName());
        }
      });
    }

    for (Map.Entry<Partition, List<Message>> entry : resourceMessageMap.entrySet()) {
      entry.getValue().forEach(e -> {
        if (!e.getToState().equals(HelixDefinedState.DROPPED.name())) {
          intermediateStateMap.setState(entry.getKey(), e.getTgtName(), e.getToState());
        } else {
          intermediateStateMap.getStateMap().get(entry.getKey()).remove(e.getTgtName());
        }
      });
    }
  }
}
