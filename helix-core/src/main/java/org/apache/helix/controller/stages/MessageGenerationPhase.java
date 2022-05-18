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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.api.config.StateTransitionTimeoutConfig;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.MessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compares the currentState, pendingState with IdealState and generate messages
 */
public class MessageGenerationPhase extends AbstractBaseStage {
  private final static String NO_DESIRED_STATE = "NoDesiredState";

  // If we see there is any invalid pending message leaving on host, i.e. message
  // tells participant to change from SLAVE to MASTER, and the participant is already
  // at MASTER state, we wait for timeout and if the message is still not cleaned up by
  // participant, controller will cleanup them proactively to unblock further state
  // transition
  public final static long DEFAULT_OBSELETE_MSG_PURGE_DELAY = HelixUtil
      .getSystemPropertyAsLong(SystemPropertyKeys.CONTROLLER_MESSAGE_PURGE_DELAY, 60 * 1000);
  private final static String PENDING_MESSAGE = "pending message";
  private final static String STALE_MESSAGE = "stale message";

  private static Logger logger = LoggerFactory.getLogger(MessageGenerationPhase.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    _eventId = event.getEventId();
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    BaseControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    CurrentStateOutput currentStateOutput = event.getAttribute(AttributeName.CURRENT_STATE.name());

    Map<String, Map<String, Message>> messagesToCleanUp = new HashMap<>();
    if (manager == null || cache == null || resourceMap == null || currentStateOutput == null
        || bestPossibleStateOutput == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager|DataCache|RESOURCES|CURRENT_STATE|BESTPOSSIBLE_STATE");
    }

    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    Map<String, String> sessionIdMap = new HashMap<>();

    for (LiveInstance liveInstance : liveInstances.values()) {
      sessionIdMap.put(liveInstance.getInstanceName(), liveInstance.getEphemeralOwner());
    }
    MessageOutput output = new MessageOutput();

    for (Resource resource : resourceMap.values()) {
      try {
        generateMessage(resource, cache, bestPossibleStateOutput, currentStateOutput, manager,
            sessionIdMap, event.getEventType(), output, messagesToCleanUp);
      } catch (HelixException ex) {
        LogUtil.logError(logger, _eventId,
            "Failed to generate message for resource " + resource.getResourceName(), ex);
      }
    }

    // Asynchronously GC pending messages if necessary
    if (!messagesToCleanUp.isEmpty()) {
      schedulePendingMessageCleanUp(messagesToCleanUp, cache.getAsyncTasksThreadPool(),
          manager.getHelixDataAccessor());
    }
    event.addAttribute(AttributeName.MESSAGES_ALL.name(), output);
  }

  private void generateMessage(final Resource resource, final BaseControllerDataProvider cache,
      final ResourcesStateMap resourcesStateMap, final CurrentStateOutput currentStateOutput,
      final HelixManager manager, final Map<String, String> sessionIdMap,
      final ClusterEventType eventType, MessageOutput output,
      Map<String, Map<String, Message>> messagesToCleanUp) {
    String resourceName = resource.getResourceName();

    StateModelDefinition stateModelDef = cache.getStateModelDef(resource.getStateModelDefRef());
    if (stateModelDef == null) {
      LogUtil.logError(logger, _eventId,
          "State Model Definition null, skip generating messages for resource: " + resourceName);
      return;
    }

    for (Partition partition : resource.getPartitions()) {
      Map<String, String> instanceStateMap =
          new HashMap<>(resourcesStateMap.getInstanceStateMap(resourceName, partition));
      Map<String, String> pendingStateMap =
          currentStateOutput.getPendingStateMap(resourceName, partition);

      // The operation is combing pending state with best possible state. Since some replicas have
      // been moved from one instance to another, the instance will exist in pending state but not
      // best possible. Thus Helix need to check the pending state and cancel it.

      for (String instance : pendingStateMap.keySet()) {
        if (!instanceStateMap.containsKey(instance)) {
          instanceStateMap.put(instance, NO_DESIRED_STATE);
        }
      }

      // we should generate message based on the desired-state priority
      // so keep generated messages in a temp map keyed by state
      // desired-state->list of generated-messages
      Map<String, List<Message>> messageMap = new HashMap<>();

      for (String instanceName : instanceStateMap.keySet()) {

        Set<Message> staleMessages = cache.getStaleMessagesByInstance(instanceName);

        String desiredState = instanceStateMap.get(instanceName);

        String currentState =
            currentStateOutput.getCurrentState(resourceName, partition, instanceName);
        Message pendingMessage =
            currentStateOutput.getPendingMessage(resourceName, partition, instanceName);
        boolean isCancellationEnabled = cache.getClusterConfig().isStateTransitionCancelEnabled();
        Message cancellationMessage =
            currentStateOutput.getCancellationMessage(resourceName, partition, instanceName);
        String nextState = stateModelDef.getNextStateForTransition(currentState, desiredState);

        Message message = null;

        if (currentState == null) {
          currentState = stateModelDef.getInitialState();
          nextState = stateModelDef.getNextStateForTransition(currentState, desiredState);

          if (desiredState.equals(HelixDefinedState.DROPPED.name())) {
            LogUtil.logDebug(logger, _eventId, String
                .format("No current state for partition %s in resource %s, skip the drop message",
                    partition.getPartitionName(), resourceName));

            message =
                generateCancellationMessageForPendingMessage(desiredState, currentState, nextState,
                    pendingMessage, manager, resource, partition, sessionIdMap, instanceName,
                    stateModelDef, cancellationMessage, isCancellationEnabled);
            addGeneratedMessageToMap(message, messageMap, eventType, cache, desiredState,
                resourceName, partition, currentState, nextState);

            // TODO: separate logic of resource/task message generation
            if (cache instanceof ResourceControllerDataProvider) {
              ((ResourceControllerDataProvider) cache)
                  .invalidateCachedIdealStateMapping(resourceName);
            }
            continue;
          }
        }

        if (shouldCleanUpPendingMessage(pendingMessage, sessionIdMap, instanceName, currentState,
            currentStateOutput.getEndTime(resourceName, partition, instanceName))) {
          logAndAddToCleanUp(messagesToCleanUp, pendingMessage, instanceName, resourceName,
              partition, currentState, PENDING_MESSAGE);
        }

        for (Message staleMessage : staleMessages) {
          // staleMessage can be simple or batch mode
          if ((System.currentTimeMillis() - currentStateOutput
              .getEndTime(resourceName, partition, instanceName) > DEFAULT_OBSELETE_MSG_PURGE_DELAY)
              && staleMessage.getResourceName().equals(resourceName) && sessionIdMap
              .containsKey(instanceName) && (
              staleMessage.getPartitionName().equals(partition.getPartitionName()) || (
                  staleMessage.getBatchMessageMode() && staleMessage.getPartitionNames()
                      .contains(partition.getPartitionName())))) {
            logAndAddToCleanUp(messagesToCleanUp, staleMessage, instanceName, resourceName,
                partition, currentState, STALE_MESSAGE);
          }
        }

        if (desiredState.equals(NO_DESIRED_STATE) || desiredState.equalsIgnoreCase(currentState)) {
          if (shouldCreateSTCancellation(pendingMessage, desiredState,
              stateModelDef.getInitialState())) {
            message = MessageUtil
                .createStateTransitionCancellationMessage(manager.getInstanceName(),
                    manager.getSessionId(), resource, partition.getPartitionName(), instanceName,
                    sessionIdMap.get(instanceName), stateModelDef.getId(),
                    pendingMessage.getFromState(), pendingMessage.getToState(), null,
                    cancellationMessage, isCancellationEnabled, currentState);
          }
        } else {
          if (nextState == null) {
            LogUtil.logError(logger, _eventId,
                "Unable to find a next state for resource: " + resource.getResourceName()
                    + " partition: " + partition.getPartitionName() + " from stateModelDefinition"
                    + stateModelDef.getClass() + " from:" + currentState + " to:" + desiredState);
            continue;
          }

          if (pendingMessage != null) {
            message =
                generateCancellationMessageForPendingMessage(desiredState, currentState, nextState,
                    pendingMessage, manager, resource, partition, sessionIdMap, instanceName,
                    stateModelDef, cancellationMessage, isCancellationEnabled);
          } else {
            // Create new state transition message
            message = MessageUtil
                .createStateTransitionMessage(manager.getInstanceName(), manager.getSessionId(),
                    resource, partition.getPartitionName(), instanceName, currentState, nextState,
                    sessionIdMap.get(instanceName), stateModelDef.getId());

            if (logger.isDebugEnabled()) {
              LogUtil.logDebug(logger, _eventId, String.format(
                  "Resource %s partition %s for instance %s with currentState %s and nextState %s",
                  resource.getResourceName(), partition.getPartitionName(), instanceName,
                  currentState, nextState));
            }
          }
        }
        addGeneratedMessageToMap(message, messageMap, eventType, cache, desiredState, resourceName,
            partition, currentState, nextState);
      }

      // add generated messages to output according to state priority
      List<String> statesPriorityList = stateModelDef.getStatesPriorityList();
      for (String state : statesPriorityList) {
        if (messageMap.containsKey(state)) {
          for (Message message : messageMap.get(state)) {
            // This is for a bug where a message's target session id is null
            if (!message.isValid()) {
              LogUtil.logError(logger, _eventId, String.format(
                  "An invalid message was generated! Discarding this message. sessionIdMap: %s, CurrentStateMap: %s, InstanceStateMap: %s, AllInstances: %s, LiveInstances: %s, Message: %s",
                  sessionIdMap, currentStateOutput.getCurrentStateMap(resourceName, partition),
                  instanceStateMap, cache.getAllInstances(), cache.getLiveInstances().keySet(),
                  message));
              continue; // Do not add this message
            }
            output.addMessage(resourceName, partition, message);
          }
        }
      }
    } // end of for-each-partition
  }

  private boolean shouldCreateSTCancellation(Message pendingMessage, String desiredState,
      String initialState) {
    if (pendingMessage == null) {
      return false;
    }
    if (NO_DESIRED_STATE.equals(desiredState)) {
      return true;
    }

    // Cancel the ST except below scenarios:
    // 1. pending message toState is desired state
    // 2. pending message is an ERROR reset: ERROR -> initState (eg. OFFLINE)
    return !desiredState.equalsIgnoreCase(pendingMessage.getToState()) && !(
        HelixDefinedState.ERROR.name().equals(pendingMessage.getFromState()) && initialState
            .equals(pendingMessage.getToState()));
  }

  private void logAndAddToCleanUp(Map<String, Map<String, Message>> messagesToCleanUp,
      Message message, String instanceName, String resourceName, Partition partition,
      String currentState, String cleanUpMessageType) {
    String logMsg = String.format(
        "Adding %s %s on instance %s to clean up. Msg: %s->%s, current state"
            + " of resource %s:%s is %s", cleanUpMessageType, message.getMsgId(), instanceName,
        message.getFromState(), message.getToState(), resourceName, partition, currentState);
    LogUtil.logInfo(logger, _eventId, logMsg);
    if (!messagesToCleanUp.containsKey(instanceName)) {
      messagesToCleanUp.put(instanceName, new HashMap<String, Message>());
    }
    messagesToCleanUp.get(instanceName).put(message.getMsgId(), message);
  }

  private Message generateCancellationMessageForPendingMessage(final String desiredState,
      final String currentState, final String nextState, final Message pendingMessage,
      final HelixManager manager, final Resource resource, final Partition partition,
      final Map<String, String> sessionIdMap, final String instanceName,
      final StateModelDefinition stateModelDef, final Message cancellationMessage,
      final boolean isCancellationEnabled) {

    Message message = null;

    if (pendingMessage != null) {
      String pendingState = pendingMessage.getToState();
      if (nextState.equalsIgnoreCase(pendingState)) {
        LogUtil.logInfo(logger, _eventId,
            "Message already exists for " + instanceName + " to transit " + resource
                .getResourceName() + "." + partition.getPartitionName() + " from " + currentState
                + " to " + nextState + ", isRelay: " + pendingMessage.isRelayMessage());
      } else if (currentState.equalsIgnoreCase(pendingState)) {
        LogUtil.logDebug(logger, _eventId,
            "Message hasn't been removed for " + instanceName + " to transit " + resource
                .getResourceName() + "." + partition.getPartitionName() + " to " + pendingState
                + ", desiredState: " + desiredState + ", isRelay: " + pendingMessage
                .isRelayMessage());
      } else {
        LogUtil.logDebug(logger, _eventId,
            "IdealState changed before state transition completes for " + resource.getResourceName()
                + "." + partition.getPartitionName() + " on " + instanceName + ", pendingState: "
                + pendingState + ", currentState: " + currentState + ", nextState: " + nextState
                + ", isRelay: " + pendingMessage.isRelayMessage());

        message = MessageUtil.createStateTransitionCancellationMessage(manager.getInstanceName(),
            manager.getSessionId(), resource, partition.getPartitionName(), instanceName,
            sessionIdMap.get(instanceName), stateModelDef.getId(), pendingMessage.getFromState(),
            pendingState, nextState, cancellationMessage, isCancellationEnabled, currentState);
      }
    }
    return message;
  }

  private void addGeneratedMessageToMap(final Message message,
      Map<String, List<Message>> messageMap, final ClusterEventType eventType,
      final BaseControllerDataProvider cache, final String desiredState, final String resourceName,
      final Partition partition, final String currentState, final String nextState) {
    if (message != null) {
      IdealState idealState = cache.getIdealState(resourceName);
      if (idealState != null && idealState.getStateModelDefRef()
          .equalsIgnoreCase(DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE)) {
        if (idealState.getRecord().getMapField(partition.getPartitionName()) != null) {
          message.getRecord().setMapField(Message.Attributes.INNER_MESSAGE.toString(),
              idealState.getRecord().getMapField(partition.getPartitionName()));
        }
      }

      int timeout =
          getTimeOut(cache.getClusterConfig(), cache.getResourceConfig(resourceName), currentState,
              nextState, idealState, partition);
      if (timeout > 0) {
        message.setExecutionTimeout(timeout);
      }

      message.setAttribute(Message.Attributes.ClusterEventName, eventType.name());
      // output.addMessage(resourceName, partition, message);
      if (!messageMap.containsKey(desiredState)) {
        messageMap.put(desiredState, new ArrayList<Message>());
      }
      messageMap.get(desiredState).add(message);
    }
  }

  /**
   * Start a job in worker pool that asynchronously clean up pending message. Since it is possible
   * that participant failed to clean up message after processing, it is important for controller to
   * try to clean them up as well to unblock further rebalance
   *
   * @param pendingMessagesToPurge key: instance name, value: list of pending message to cleanup
   * @param workerPool             ExecutorService that job can be submitted to
   * @param accessor               Data accessor used to clean up message
   */
  private void schedulePendingMessageCleanUp(
      final Map<String, Map<String, Message>> pendingMessagesToPurge, ExecutorService workerPool,
      final HelixDataAccessor accessor) {
    workerPool.submit(new Callable<Object>() {
      @Override
      public Object call() {
        for (Map.Entry<String, Map<String, Message>> entry : pendingMessagesToPurge.entrySet()) {
          String instanceName = entry.getKey();
          for (Message msg : entry.getValue().values()) {
            if (accessor.removeProperty(msg.getKey(accessor.keyBuilder(), instanceName))) {
              LogUtil.logInfo(logger, _eventId, String
                  .format("Deleted message %s from instance %s", msg.getMsgId(), instanceName));
            }
          }
        }
        return null;
      }
    });
  }

  private boolean shouldCleanUpPendingMessage(Message pendingMsg, Map<String, String> sessionIdMap,
      String instanceName, String currentState, Long currentStateTransitionEndTime) {
    if (pendingMsg == null || !sessionIdMap.containsKey(instanceName)) {
      return false;
    }
    if (currentState.equalsIgnoreCase(pendingMsg.getToState())) {
      // If pending message's toState is same as current state, state transition is finished
      // successfully. In this case, we will wait for a timeout for participant to cleanup
      // processed message. If participant fail to do so, controller is going to proactively delete
      // the message as participant does not retry message deletion upon failure.
      return System.currentTimeMillis() - currentStateTransitionEndTime
          > DEFAULT_OBSELETE_MSG_PURGE_DELAY;
    } else {
      // Partition's current state should be either pending message's fromState or toState or
      // the message is invalid and can be safely deleted immediately.
      return !currentState.equalsIgnoreCase(pendingMsg.getFromState());
    }
  }

  private int getTimeOut(ClusterConfig clusterConfig, ResourceConfig resourceConfig,
      String currentState, String nextState, IdealState idealState, Partition partition) {
    StateTransitionTimeoutConfig stateTransitionTimeoutConfig =
        clusterConfig.getStateTransitionTimeoutConfig();
    int timeout = stateTransitionTimeoutConfig != null ? stateTransitionTimeoutConfig
        .getStateTransitionTimeout(currentState, nextState) : -1;

    String timeOutStr = null;
    // Check IdealState whether has timeout set
    if (idealState != null) {
      String stateTransition = currentState + "-" + nextState + "_" + Message.Attributes.TIMEOUT;
      timeOutStr = idealState.getRecord().getSimpleField(stateTransition);
      if (timeOutStr == null && idealState.getStateModelDefRef()
          .equalsIgnoreCase(DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE)) {
        // scheduled task queue
        if (idealState.getRecord().getMapField(partition.getPartitionName()) != null) {
          timeOutStr = idealState.getRecord().getMapField(partition.getPartitionName())
              .get(Message.Attributes.TIMEOUT.toString());
        }
      }
    }
    if (timeOutStr != null) {
      try {
        timeout = Integer.parseInt(timeOutStr);
      } catch (Exception e) {
        LogUtil.logError(logger, _eventId, "", e);
      }
    }

    if (resourceConfig != null) {
      // If resource config has timeout, replace the cluster timeout.
      stateTransitionTimeoutConfig = resourceConfig.getStateTransitionTimeoutConfig();
      timeout = stateTransitionTimeoutConfig != null ? stateTransitionTimeoutConfig
          .getStateTransitionTimeout(currentState, nextState) : -1;
    }

    return timeout;
  }
}
