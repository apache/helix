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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes necessary cluster events for cluster management mode.
 */
public class ManagementModeStage extends AbstractBaseStage {
  private static final Logger LOG = LoggerFactory.getLogger(ManagementModeStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
  }

  private Message createStatusChangeMessage(String srcInstanceName, String tgtInstanceName,
      LiveInstance.LiveInstanceStatus toStatus, String sessionId, String managerSessionId) {
    String uuid = UUID.randomUUID().toString();
    Message message = new Message(Message.MessageType.PARTICIPANT_STATUS_CHANGE, uuid);
    message.setSrcName(srcInstanceName);
    message.setTgtName(tgtInstanceName);
    message.setToStatus(toStatus);
    message.setMsgState(Message.MessageState.NEW);
    message.setTgtSessionId(sessionId);
    message.setSrcSessionId(managerSessionId);
    message.setExpectedSessionId(managerSessionId);
    return message;
  }

  // Cancellation message creation should double check cancellation feature in cluster config.
  private List<Message> createCancellationMessages(List<Message> messages, String managerSessionId,
      String srcInstance, String tgtSession,
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

  // Build cancellation messages map for all enabled live instances
  private Map<String, Map<String, Map<String, Message>>> buildCancelMessageMap(
      BaseControllerDataProvider cache) {
    Map<String, Map<String, Map<String, Message>>> cancelMessageMap = new HashMap<>();
    Set<String> enabledLiveInstances = cache.getEnabledInstances();

    for (String instance : enabledLiveInstances) {
      Map<String, Message> messages = cache.getMessages(instance);
      for (Message message : messages.values()) {
        if (Message.MessageType.STATE_TRANSITION_CANCELLATION.name()
            .equalsIgnoreCase(message.getMsgType())) {
          String resource = message.getResourceGroupName();
          String partition = message.getPartitionName();
          cancelMessageMap.computeIfAbsent(resource, map -> new HashMap<>())
              .computeIfAbsent(partition, map -> new HashMap<>()).put(instance, message);
        }
      }
    }

    return cancelMessageMap;
  }

  private Message findExistingCancelMessage(Message message,
      Map<String, Map<String, Map<String, Message>>> cancelMessageMap) {
    Map<String, Map<String, Message>> map = cancelMessageMap.get(message.getResourceName());
    if (map != null) {
      Map<String, Message> instanceStateMap = map.get(message.getPartitionName());
      if (instanceStateMap != null) {
        return instanceStateMap.get(message.getTgtName());
      }
    }
    return null;
  }

  private Message createSTCancelMessage(Message pendingMessage, Message exitingCancelMessage,
      String managerSessionId, String srcInstance, String tgtSessionId) {
    // Ignore non-ST or error reset messages
    if (pendingMessage == null || exitingCancelMessage != null
        || !Message.MessageType.STATE_TRANSITION.name().equalsIgnoreCase(pendingMessage.getMsgType())
        || HelixDefinedState.ERROR.name().equals(pendingMessage.getFromState())) {
      return null;
    }

    LOG.info("Event {} : Send cancellation message of the state transition for {}.{} on {}, "
            + "currentState: {},  toState: {}", _eventId, pendingMessage.getResourceName(),
        pendingMessage.getPartitionName(), pendingMessage.getTgtName(),
        pendingMessage.getFromState(), pendingMessage.getToState());

    String uuid = UUID.randomUUID().toString();
    Message cancelMessage = new Message(Message.MessageType.STATE_TRANSITION_CANCELLATION, uuid);
    cancelMessage.setSrcName(srcInstance);
    cancelMessage.setTgtName(pendingMessage.getTgtName());
    cancelMessage.setMsgState(Message.MessageState.NEW);
    cancelMessage.setPartitionName(pendingMessage.getPartitionName());
    cancelMessage.setResourceName(pendingMessage.getResourceName());
    cancelMessage.setFromState(pendingMessage.getFromState());
    cancelMessage.setToState(pendingMessage.getToState());
    cancelMessage.setTgtSessionId(tgtSessionId);
    cancelMessage.setSrcSessionId(managerSessionId);
    cancelMessage.setExpectedSessionId(managerSessionId);
    cancelMessage.setStateModelDef(pendingMessage.getStateModelDef());
    cancelMessage.setStateModelFactoryName(pendingMessage.getStateModelFactoryName());
    cancelMessage.setBucketSize(pendingMessage.getBucketSize());

    return cancelMessage;
  }

  private List<Message> dispatchMessages(HelixDataAccessor accessor, List<Message> messages) {
    if (messages.isEmpty()) {
      return Collections.emptyList();
    }

    List<Message> messagesSent = new ArrayList<>();
    List<PropertyKey> keys = new ArrayList<>(messages.size());
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    for (Message message : messages) {
      LogUtil.logInfo(LOG, _eventId,
          "Sending Message " + message.getMsgId() + " to " + message.getTgtName()
              + " changing status to " + message.getToStatus());
      keys.add(keyBuilder.message(message.getTgtName(), message.getId()));
    }

    boolean[] results = accessor.createChildren(keys, messages);
    for (int i = 0; i < results.length; i++) {
      if (!results[i]) {
        LogUtil.logError(LOG, _eventId, "Failed to send message: " + keys.get(i));
      } else {
        messagesSent.add(messages.get(i));
      }
    }

    return messagesSent;
  }
}
