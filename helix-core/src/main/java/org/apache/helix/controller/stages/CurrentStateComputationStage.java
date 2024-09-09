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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.rebalancer.util.ResourceUsageCalculator;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.rebalancer.waged.WagedInstanceCapacity;
import org.apache.helix.controller.rebalancer.waged.WagedResourceWeightsProvider;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
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
  private boolean _isTaskFrameworkPipeline = false;

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    BaseControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());

    // TODO: remove the explicit checking of type, since this could potentially complicates
    //  pipeline separation
    if (cache instanceof WorkflowControllerDataProvider) {
      _isTaskFrameworkPipeline = true;
    }

    final Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    final Map<String, Resource> resourceToRebalance =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());

    if (cache == null || resourceMap == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires DataCache|RESOURCE");
    }

    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    final CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    final CurrentStateOutput currentStateExcludingUnknown = new CurrentStateOutput();

    for (LiveInstance instance : liveInstances.values()) {
      String instanceName = instance.getInstanceName();
      String instanceSessionId = instance.getEphemeralOwner();
      InstanceConfig instanceConfig = cache.getInstanceConfigMap().get(instanceName);

      Set<Message> existingStaleMessages = cache.getStaleMessagesByInstance(instanceName);
      Map<String, Message> messages = cache.getMessages(instanceName);
      Map<String, Message> relayMessages = cache.getRelayMessages(instanceName);

      // update current states.
      updateCurrentStates(instance,
          cache.getCurrentState(instanceName, instanceSessionId, _isTaskFrameworkPipeline).values(),
          currentStateOutput, resourceMap);
      // update pending messages
      updatePendingMessages(instance, cache, messages.values(), relayMessages.values(),
          existingStaleMessages, currentStateOutput, resourceMap);

      // Only update the currentStateExcludingUnknown if the instance is not in UNKNOWN InstanceOperation.
      if (instanceConfig == null || !instanceConfig.getInstanceOperation()
          .getOperation()
          .equals(InstanceConstants.InstanceOperation.UNKNOWN)) {
        // update current states.
        updateCurrentStates(instance,
            cache.getCurrentState(instanceName, instanceSessionId, _isTaskFrameworkPipeline)
                .values(), currentStateExcludingUnknown, resourceMap);
        // update pending messages
        updatePendingMessages(instance, cache, messages.values(), relayMessages.values(),
            existingStaleMessages, currentStateExcludingUnknown, resourceMap);
      }
    }
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateExcludingUnknown);

    final ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    if (clusterStatusMonitor != null && cache instanceof ResourceControllerDataProvider) {
      final ResourceControllerDataProvider dataProvider = (ResourceControllerDataProvider) cache;
      reportInstanceCapacityMetrics(clusterStatusMonitor, dataProvider, resourceToRebalance,
          currentStateOutput);
      reportResourcePartitionCapacityMetrics(dataProvider.getAsyncTasksThreadPool(),
          clusterStatusMonitor, dataProvider.getResourceConfigMap().values());

      handleResourceCapacityCalculation(event, (ResourceControllerDataProvider) cache, currentStateOutput);
    }

    // Populate the capacity for simple CapacityNode
    if (cache.getClusterConfig() != null
        && cache.getClusterConfig().getGlobalMaxPartitionAllowedPerInstance() != -1
        && cache instanceof ResourceControllerDataProvider) {
      final ResourceControllerDataProvider dataProvider = (ResourceControllerDataProvider) cache;
      dataProvider.populateSimpleCapacitySetUsage(resourceToRebalance.keySet(),
          currentStateExcludingUnknown);
    }
  }

  // update all pending messages to CurrentStateOutput.
  private void updatePendingMessages(LiveInstance instance, BaseControllerDataProvider cache,
      Collection<Message> pendingMessages, Collection<Message> pendingRelayMessages,
      Set<Message> existingStaleMessages, CurrentStateOutput currentStateOutput,
      Map<String, Resource> resourceMap) {
    String instanceName = instance.getInstanceName();
    String instanceSessionId = instance.getEphemeralOwner();

    // update all pending messages
    for (Message message : pendingMessages) {
      // ignore existing stale messages
      if (existingStaleMessages.contains(message)) {
        continue;
      }
      if (!MessageType.STATE_TRANSITION.name().equalsIgnoreCase(message.getMsgType())
          && !MessageType.STATE_TRANSITION_CANCELLATION.name()
          .equalsIgnoreCase(message.getMsgType())) {
        continue;
      }
      if (!instanceSessionId.equals(message.getTgtSessionId())) {
        continue;
      }
      String resourceName = message.getResourceName();
      Resource resource = resourceMap.get(resourceName);
      if (resource == null) {
        LogUtil.logDebug(LOG, _eventId, String.format(
            "Ignore a pending relay message %s for a non-exist resource %s and partition %s",
            message.getMsgId(), resourceName, message.getPartitionName()));
        continue;
      }

      if (!message.getBatchMessageMode()) {
        String partitionName = message.getPartitionName();
        Partition partition = resource.getPartition(partitionName);
        if (partition != null) {
          String currentState = currentStateOutput.getCurrentState(resourceName, partition,
              instanceName);
          if (_isTaskFrameworkPipeline || !isStaleMessage(message, currentState)) {
            setMessageState(currentStateOutput, resourceName, partition, instanceName, message);
          } else {
            cache.addStaleMessage(instanceName, message);
          }
        } else {
          LogUtil.logDebug(LOG, _eventId, String
              .format("Ignore a pending message %s for a non-exist resource %s and partition %s",
                  message.getMsgId(), resourceName, message.getPartitionName()));
        }
      } else {
        List<String> partitionNames = message.getPartitionNames();
        if (!partitionNames.isEmpty()) {
          for (String partitionName : partitionNames) {
            Partition partition = resource.getPartition(partitionName);
            if (partition != null) {
              setMessageState(currentStateOutput, resourceName, partition, instanceName, message);
            } else {
              LogUtil.logDebug(LOG, _eventId, String.format(
                  "Ignore a pending message %s for a non-exist resource %s and partition %s",
                  message.getMsgId(), resourceName, message.getPartitionName()));
            }
          }
        }
      }

      // Add the state model into the map for lookup of Task Framework pending partitions
      if (resource.getStateModelDefRef() != null) {
        currentStateOutput.setResourceStateModelDef(resourceName, resource.getStateModelDefRef());
      }
    }


    // update all pending relay messages
    for (Message message : pendingRelayMessages) {
      if (!message.isRelayMessage()) {
        LogUtil.logWarn(LOG, _eventId,
            String.format("Not a relay message %s, ignored!", message.getMsgId()));
        continue;
      }
      String resourceName = message.getResourceName();
      Resource resource = resourceMap.get(resourceName);
      if (resource == null) {
        LogUtil.logDebug(LOG, _eventId, String.format(
            "Ignore a pending relay message %s for a non-exist resource %s and partition %s",
            message.getMsgId(), resourceName, message.getPartitionName()));
        continue;
      }

      if (!message.getBatchMessageMode()) {
        String partitionName = message.getPartitionName();
        Partition partition = resource.getPartition(partitionName);
        if (partition != null) {
          currentStateOutput.setPendingRelayMessage(resourceName, partition, instanceName, message);
        } else {
          LogUtil.logDebug(LOG, _eventId, String.format(
              "Ignore a pending relay message %s for a non-exist resource %s and partition %s",
              message.getMsgId(), resourceName, message.getPartitionName()));
        }
      } else {
        LogUtil.logWarn(LOG, _eventId,
            String.format("A relay message %s should not be batched, ignored!", message.getMsgId()));
      }
    }
  }

  private boolean isStaleMessage(Message message, String currentState) {
    if (currentState == null || message.getFromState() == null || message.getToState() == null) {
      return false;
    }
    return !message.getFromState().equalsIgnoreCase(currentState) || message.getToState()
        .equalsIgnoreCase(currentState);
  }

  // update current states in CurrentStateOutput
  private void updateCurrentStates(LiveInstance instance, Collection<CurrentState> currentStates,
      CurrentStateOutput currentStateOutput, Map<String, Resource> resourceMap) {
    String instanceName = instance.getInstanceName();
    String instanceSessionId = instance.getEphemeralOwner();

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
          currentStateOutput.setEndTime(resourceName, partition, instanceName,
              currentState.getEndTime(partitionName));
          String info = currentState.getInfo(partitionName);
          // This is to avoid null value entries in the map, and reduce memory usage by avoiding extra empty entries in the map.
          if (info != null) {
            currentStateOutput.setInfo(resourceName, partition, instanceName, info);
          }
          String requestState = currentState.getRequestedState(partitionName);
          if (requestState != null) {
            currentStateOutput
                .setRequestedState(resourceName, partition, instanceName, requestState);
          }
        }
      }
    }
  }

  private void setMessageState(CurrentStateOutput currentStateOutput, String resourceName,
      Partition partition, String instanceName, Message message) {
    if (MessageType.STATE_TRANSITION.name().equalsIgnoreCase(message.getMsgType())) {
      currentStateOutput.setPendingMessage(resourceName, partition, instanceName, message);
    } else {
      currentStateOutput.setCancellationMessage(resourceName, partition, instanceName, message);
    }
  }

  private void reportInstanceCapacityMetrics(ClusterStatusMonitor clusterStatusMonitor,
      ResourceControllerDataProvider dataProvider, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput) {
    asyncExecute(dataProvider.getAsyncTasksThreadPool(), () -> {
      try {
        // ResourceToRebalance map also has resources from current states.
        // Only use the resources in ideal states that enable WAGED to parse all replicas.
        Map<String, IdealState> idealStateMap = dataProvider.getIdealStates();
        Map<String, Resource> resourceToMonitorMap = resourceMap.entrySet().stream()
            .filter(entry -> WagedValidationUtil.isWagedEnabled(idealStateMap.get(entry.getKey())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, ResourceAssignment> currentStateAssignment =
            currentStateOutput.getAssignment(resourceToMonitorMap.keySet());
        ClusterModel clusterModel = ClusterModelProvider.generateClusterModelFromExistingAssignment(
            dataProvider, resourceToMonitorMap, currentStateAssignment);

        for (AssignableNode node : clusterModel.getAssignableNodes().values()) {
          String instanceName = node.getInstanceName();
          // There is no new usage adding to this node, so an empty map is passed in.
          double usage = node.getGeneralProjectedHighestUtilization(Collections.emptyMap());
          clusterStatusMonitor
              .updateInstanceCapacityStatus(instanceName, usage, node.getMaxCapacity());
        }
      } catch (Exception ex) {
        LOG.error("Failed to report instance capacity metrics. Exception message: {}",
            ex.getMessage());
      }

      return null;
    });
  }

  private void reportResourcePartitionCapacityMetrics(ExecutorService executorService,
      ClusterStatusMonitor clusterStatusMonitor, Collection<ResourceConfig> resourceConfigs) {
    asyncExecute(executorService, () -> {
      try {
        for (ResourceConfig config : resourceConfigs) {
          Map<String, Integer> averageWeight = ResourceUsageCalculator
              .calculateAveragePartitionWeight(config.getPartitionCapacityMap());
          clusterStatusMonitor.updatePartitionWeight(config.getResourceName(), averageWeight);
        }
      } catch (Exception ex) {
        LOG.error("Failed to report resource partition capacity metrics. Exception message: {}",
            ex.getMessage());
      }

      return null;
    });
  }

  void handleResourceCapacityCalculation(ClusterEvent event, ResourceControllerDataProvider cache,
      CurrentStateOutput currentStateOutput) {
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    if (skipCapacityCalculation(cache, resourceMap, event)) {
      return;
    }

    Map<String, Resource> wagedEnabledResourceMap = resourceMap.entrySet()
        .parallelStream()
        .filter(entry -> WagedValidationUtil.isWagedEnabled(cache.getIdealState(entry.getKey())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (wagedEnabledResourceMap.isEmpty()) {
      return;
    }

    // Phase 1: Rebuild Always
    WagedInstanceCapacity capacityProvider = new WagedInstanceCapacity(cache);
    WagedResourceWeightsProvider weightProvider = new WagedResourceWeightsProvider(cache);

    capacityProvider.process(cache, currentStateOutput, wagedEnabledResourceMap, weightProvider);
    cache.setWagedCapacityProviders(capacityProvider, weightProvider);
  }

  /**
   * Function that checks whether we should return early, without any action on the capacity map or not.
   *
   * @param cache it is the cluster level cache for the resources.
   * @param event the cluster event that is undergoing processing.
   * @return true, of the condition evaluate to true and no action is needed, else false.
   */
  static boolean skipCapacityCalculation(ResourceControllerDataProvider cache, Map<String, Resource> resourceMap,
      ClusterEvent event) {
    if (resourceMap == null || resourceMap.isEmpty()) {
      return true;
    }

    if (Objects.isNull(cache.getWagedInstanceCapacity())) {
      return false;
    }

    // TODO: We will change this logic to handle each event-type differently and depending on the resource type.
    switch (event.getEventType()) {
      case ClusterConfigChange:
      case InstanceConfigChange:
      case ResourceConfigChange:
      case ControllerChange:
      case LiveInstanceChange:
      case CurrentStateChange:
      case PeriodicalRebalance:
      case MessageChange:
        return false;
      default:
        return true;
    }
  }

}
