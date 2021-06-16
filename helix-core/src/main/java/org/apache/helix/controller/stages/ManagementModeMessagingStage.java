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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.ManagementControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.LiveInstance.LiveInstanceStatus;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.Resource;
import org.apache.helix.util.MessageUtil;
import org.apache.helix.util.RebalanceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates and sends messages to participants for management mode pipeline.
 */
public class ManagementModeMessagingStage extends AbstractBaseStage {
  private static final Logger LOG = LoggerFactory.getLogger(ManagementModeMessagingStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null) {
      throw new StageException("HelixManager attribute value is null");
    }

    ClusterManagementMode managementMode = event.getAttribute(AttributeName.MANAGEMENT_MODE.name());

    // Still in progress? Check and send messages
    if (!ClusterManagementMode.Status.COMPLETED.equals(managementMode.getStatus())) {
      ManagementControllerDataProvider cache =
          event.getAttribute(AttributeName.ControllerDataProvider.name());

      List<Message> messagesToSend =
          generateMessages(managementMode.getMode(), cache.getEnabledLiveInstances(),
              cache.getLiveInstances(), cache.getAllInstancesMessages(), manager.getInstanceName(),
              manager.getSessionId(), cache.getPauseSignal(),
              cache.getClusterConfig().isStateTransitionCancelEnabled());

      List<Message> messagesSent =
          MessageUtil.sendMessages(manager.getHelixDataAccessor(), messagesToSend, _eventId);
      cache.cacheMessages(messagesSent);
    }

    // Can exit management mode pipeline after completed in normal mode
    if (managementMode.completedInNormalMode()) {
      LogUtil.logInfo(LOG, _eventId,
          "Exiting management mode pipeline for cluster " + event.getClusterName());
      RebalanceUtil.enableManagementMode(event.getClusterName(), false);
    }
  }

  private List<Message> generateMessages(ClusterManagementMode.Type managementMode,
      Set<String> enabledLiveInstances, Map<String, LiveInstance> liveInstanceMap,
      Map<String, Collection<Message>> allInstanceMessages, String managerInstance,
      String managerSessionId, PauseSignal pauseSignal, boolean cancellationEnabled) {
    boolean cancelPendingST =
        (cancellationEnabled && pauseSignal != null && pauseSignal.getCancelPendingST());
    // Existing cancel message map: resource -> partition -> instance -> message
    Map<String, Map<String, Map<String, Message>>> cancelMessageMap = cancelPendingST
        ? buildCancelMessageMap(enabledLiveInstances, allInstanceMessages) : Collections.emptyMap();

    List<Message> messagesToSend = new ArrayList<>();

    // Check status and pending ST for all enabled live instances
    // Send freeze/cancellation messages if necessary
    for (String instanceName : enabledLiveInstances) {
      LiveInstance liveInstance = liveInstanceMap.get(instanceName);
      Collection<Message> messages = allInstanceMessages.get(instanceName);
      String sessionId = liveInstance.getEphemeralOwner();

      // Entering freeze mode
      if (ClusterManagementMode.Type.CLUSTER_PAUSE.equals(managementMode)) {
        // Send freeze message if not yet sent
        if (!hasStatusChangeMessage(messages, LiveInstanceStatus.PAUSED)) {
          // Create freeze message
          Message message = MessageUtil
              .createStatusChangeMessage(LiveInstanceStatus.NORMAL, LiveInstanceStatus.PAUSED,
                  managerInstance, managerSessionId, instanceName, sessionId);
          messagesToSend.add(message);
        }

        // cancel pending ST
        if (cancelPendingST && hasPendingStateTransitionMessage(messages)) {
          messagesToSend.addAll(
              createCancellationMessages(messages, managerSessionId, managerInstance, sessionId,
                  cancelMessageMap));
        }
      } else if (ClusterManagementMode.Type.NORMAL.equals(managementMode)) {
        // Exiting freeze mode
        if (LiveInstanceStatus.PAUSED.equals(liveInstance.getStatus())
            && !hasStatusChangeMessage(messages, LiveInstanceStatus.NORMAL)) {
          // Send unfreeze message
          Message statusChangeMessage = MessageUtil
              .createStatusChangeMessage(LiveInstanceStatus.PAUSED, LiveInstanceStatus.NORMAL,
                  managerInstance, managerSessionId, instanceName,
                  liveInstance.getEphemeralOwner());
          messagesToSend.add(statusChangeMessage);
        }
      }
    }

    return messagesToSend;
  }

  // Build cancellation messages map for all enabled live instances
  private Map<String, Map<String, Map<String, Message>>> buildCancelMessageMap(
      Set<String> enabledLiveInstances, Map<String, Collection<Message>> allInstanceMessages) {
    Map<String, Map<String, Map<String, Message>>> cancelMessageMap = new HashMap<>();

    for (String instance : enabledLiveInstances) {
      for (Message message : allInstanceMessages.get(instance)) {
        if (MessageType.STATE_TRANSITION_CANCELLATION.name().equals(message.getMsgType())) {
          String resource = message.getResourceGroupName();
          String partition = message.getPartitionName();
          cancelMessageMap.computeIfAbsent(resource, map -> new HashMap<>())
              .computeIfAbsent(partition, map -> new HashMap<>()).put(instance, message);
        }
      }
    }

    return cancelMessageMap;
  }

  // Cancellation message creation should double check cancellation feature in cluster config.
  private List<Message> createCancellationMessages(Collection<Message> messages,
      String managerSessionId, String srcInstance, String tgtSession,
      Map<String, Map<String, Map<String, Message>>> cancelMessageMap) {
    List<Message> cancellationMessages = new ArrayList<>();

    for (Message message : messages) {
      Message existingCancelMessage = findExistingCancelMessage(message, cancelMessageMap);
      Message cancelMessage = createSTCancelMessage(message, existingCancelMessage,
          managerSessionId, srcInstance, tgtSession);
      if (cancelMessage != null) {
        cancellationMessages.add(cancelMessage);
      }
    }

    return cancellationMessages;
  }

  private Message findExistingCancelMessage(Message message,
      Map<String, Map<String, Map<String, Message>>> cancelMessageMap) {
    return cancelMessageMap.getOrDefault(message.getResourceName(), Collections.emptyMap())
        .getOrDefault(message.getPartitionName(), Collections.emptyMap()).get(message.getTgtName());
  }

  private Message createSTCancelMessage(Message pendingMessage, Message exitingCancelMessage,
      String managerSessionId, String srcInstance, String tgtSessionId) {
    // Ignore non-ST or error reset messages
    if (pendingMessage == null || exitingCancelMessage != null
        || !MessageType.STATE_TRANSITION.name().equals(pendingMessage.getMsgType())
        || HelixDefinedState.ERROR.name().equals(pendingMessage.getFromState())) {
      return null;
    }

    Resource resource = new Resource(pendingMessage.getResourceName());
    resource.setStateModelFactoryName(pendingMessage.getStateModelFactoryName());
    resource.setBucketSize(pendingMessage.getBucketSize());

    return MessageUtil
        .createStateTransitionCancellationMessage(srcInstance, managerSessionId, resource,
            pendingMessage.getPartitionName(), pendingMessage.getTgtName(), tgtSessionId,
            pendingMessage.getStateModelDef(), pendingMessage.getFromState(),
            pendingMessage.getToState(), pendingMessage.getToState(), null, true,
            pendingMessage.getFromState());
  }

  private boolean hasPendingStateTransitionMessage(Collection<Message> messages) {
    return messages.stream().anyMatch(message ->
        MessageType.STATE_TRANSITION.name().equals(message.getMsgType()));
  }

  private boolean hasStatusChangeMessage(Collection<Message> messages,
      LiveInstanceStatus toStatus) {
    return messages.stream().anyMatch(message ->
        message.isParticipantStatusChangeType() && toStatus.name().equals(message.getToState()));
  }
}
