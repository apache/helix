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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MessageDispatchStage extends AbstractBaseStage {
  private static Logger logger = LoggerFactory.getLogger(MessageDispatchStage.class);

  protected void processEvent(ClusterEvent event, MessageOutput messageOutput) throws Exception {
    _eventId = event.getEventId();
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    BaseControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());
    Map<String, LiveInstance> liveInstanceMap = cache.getLiveInstances();

    if (manager == null || resourceMap == null || messageOutput == null || cache == null
        || liveInstanceMap == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager|RESOURCES|MESSAGES_THROTTLE|DataCache|liveInstanceMap");
    }

    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    List<Message> messagesToSend = new ArrayList<>();
    for (String resourceName : resourceMap.keySet()) {
      Resource resource = resourceMap.get(resourceName);
      for (Partition partition : resource.getPartitions()) {
        List<Message> messages = messageOutput.getMessages(resourceName, partition);
        if (messages == null || messages.isEmpty()) {
          messages = Collections.emptyList();
        }
        messagesToSend.addAll(messages);
      }
    }

    List<Message> outputMessages =
        batchMessage(dataAccessor.keyBuilder(), messagesToSend, resourceMap, liveInstanceMap,
            manager.getProperties());

    // Only expect tests' events don't have EVENT_SESSION, while all events in prod should have it.
    if (!event.containsAttribute(AttributeName.EVENT_SESSION.name())) {
      logger.info("Event {} does not have event session attribute", event.getEventId());
    } else {
      // An early check for expected leader session. If the sessions don't match, it means the
      // controller's session changes, then messages should not be sent and pipeline should stop.
      Optional<String> expectedSession = event.getAttribute(AttributeName.EVENT_SESSION.name());
      if (!expectedSession.isPresent() || !expectedSession.get().equals(manager.getSessionId())) {
        throw new StageException(String.format(
            "Event session doesn't match controller %s session! Expected session: %s, actual: %s",
            manager.getInstanceName(), expectedSession.orElse("NOT_PRESENT"), manager.getSessionId()));
      }
    }

    List<Message> messagesSent = sendMessages(dataAccessor, outputMessages);

    // TODO: Need also count messages from task rebalancer
    if (!(cache instanceof WorkflowControllerDataProvider)) {
      ClusterStatusMonitor clusterStatusMonitor =
          event.getAttribute(AttributeName.clusterStatusMonitor.name());
      if (clusterStatusMonitor != null) {
        clusterStatusMonitor.increaseMessageReceived(outputMessages);
      }
    }
    long cacheStart = System.currentTimeMillis();
    cache.cacheMessages(messagesSent);
    long cacheEnd = System.currentTimeMillis();
    LogUtil.logDebug(logger, _eventId, "Caching messages took " + (cacheEnd - cacheStart) + " ms");
  }

  List<Message> batchMessage(Builder keyBuilder, List<Message> messages,
      Map<String, Resource> resourceMap, Map<String, LiveInstance> liveInstanceMap,
      HelixManagerProperties properties) {
    // group messages by its CurrentState path + "/" + fromState + "/" + toState
    Map<String, Message> batchMessages = new HashMap<String, Message>();
    List<Message> outputMessages = new ArrayList<Message>();

    Iterator<Message> iter = messages.iterator();
    while (iter.hasNext()) {
      Message message = iter.next();
      String resourceName = message.getResourceName();
      Resource resource = resourceMap.get(resourceName);

      String instanceName = message.getTgtName();
      LiveInstance liveInstance = liveInstanceMap.get(instanceName);
      String participantVersion = null;
      if (liveInstance != null) {
        participantVersion = liveInstance.getHelixVersion();
      }

      if (resource == null || !resource.getBatchMessageMode() || participantVersion == null
          || !properties.isFeatureSupported("batch_message", participantVersion)) {
        outputMessages.add(message);
        continue;
      }

      String key =
          keyBuilder.currentState(message.getTgtName(), message.getTgtSessionId(),
              message.getResourceName()).getPath()
              + "/" + message.getFromState() + "/" + message.getToState();

      if (!batchMessages.containsKey(key)) {
        Message batchMessage = new Message(message.getRecord());
        batchMessage.setBatchMessageMode(true);
        outputMessages.add(batchMessage);
        batchMessages.put(key, batchMessage);
      }
      batchMessages.get(key).addPartitionName(message.getPartitionName());
    }

    return outputMessages;
  }

  // return the messages actually sent
  protected List<Message> sendMessages(HelixDataAccessor dataAccessor, List<Message> messages) {
    List<Message> messageSent = new ArrayList<>();
    if (messages == null || messages.isEmpty()) {
      return messageSent;
    }

    Builder keyBuilder = dataAccessor.keyBuilder();

    List<PropertyKey> keys = new ArrayList<PropertyKey>();
    for (Message message : messages) {
      LogUtil.logInfo(
          logger, _eventId,
          "Sending Message " + message.getMsgId() + " to " + message.getTgtName() + " transit "
              + message.getResourceName() + "." + message.getPartitionName() + "|" + message
              .getPartitionNames() + " from:" + message.getFromState() + " to:" + message
              .getToState() + ", relayMessages: " + message.getRelayMessages().size());

      if (message.hasRelayMessages()) {
        for (Message msg : message.getRelayMessages().values()) {
          LogUtil.logInfo(logger, _eventId,
              "Sending Relay Message " + msg.getMsgId() + " to " + msg.getTgtName() + " transit "
                  + msg.getResourceName() + "." + msg.getPartitionName() + "|" + msg
                  .getPartitionNames() + " from:" + msg.getFromState() + " to:" + msg.getToState()
                  + ", relayFrom: " + msg.getRelaySrcHost() + ", attached to message: " + message
                  .getMsgId());
        }
      }

      keys.add(keyBuilder.message(message.getTgtName(), message.getId()));
    }

    boolean[] results = dataAccessor.createChildren(keys, new ArrayList<>(messages));
    for (int i = 0; i < results.length; i++) {
      if (!results[i]) {
        LogUtil.logError(logger, _eventId, "Failed to send message: " + keys.get(i));
      } else {
        messageSent.add(messages.get(i));
      }
    }

    return messageSent;
  }
}
