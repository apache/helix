package org.apache.helix.messaging;

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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.messaging.handling.AsyncCallbackService;
import org.apache.helix.messaging.handling.HelixTaskExecutor;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.builder.ConfigScopeBuilder;
import org.apache.helix.monitoring.mbeans.MessageQueueMonitor;
import org.apache.helix.monitoring.mbeans.ParticipantStatusMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMessagingService implements ClusterMessagingService {
  private final HelixManager _manager;
  private final CriteriaEvaluator _evaluator;
  private final HelixTaskExecutor _taskExecutor;
  // TODO:rename to factory, this is not a service
  private final AsyncCallbackService _asyncCallbackService;

  private static Logger _logger = LoggerFactory.getLogger(DefaultMessagingService.class);
  ConcurrentHashMap<String, MessageHandlerFactory> _messageHandlerFactoriestobeAdded =
      new ConcurrentHashMap<>();

  public DefaultMessagingService(HelixManager manager) {
    _manager = manager;
    _evaluator = new CriteriaEvaluator();

    boolean isParticipant = false;
    if (manager.getInstanceType() == InstanceType.PARTICIPANT || manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
      isParticipant = true;
    }

    _taskExecutor = new HelixTaskExecutor(
        new ParticipantStatusMonitor(isParticipant, manager.getInstanceName()),
        new MessageQueueMonitor(manager.getClusterName(), manager.getInstanceName()));
    _asyncCallbackService = new AsyncCallbackService();
    _taskExecutor.registerMessageHandlerFactory(MessageType.TASK_REPLY.name(),
        _asyncCallbackService);
  }

  @Override
  public int send(Criteria recipientCriteria, final Message messageTemplate) {
    return send(recipientCriteria, messageTemplate, null, -1);
  }

  @Override
  public int send(final Criteria recipientCriteria, final Message message,
      AsyncCallback callbackOnReply, int timeOut) {
    return send(recipientCriteria, message, callbackOnReply, timeOut, 0);
  }

  @Override
  public int send(final Criteria recipientCriteria, final Message message,
      AsyncCallback callbackOnReply, int timeOut, int retryCount) {
    Map<InstanceType, List<Message>> generateMessage = generateMessage(recipientCriteria, message);
    int totalMessageCount = 0;
    for (List<Message> messages : generateMessage.values()) {
      totalMessageCount += messages.size();
    }
    _logger.info("Send " + totalMessageCount + " messages with criteria " + recipientCriteria);
    if (totalMessageCount == 0) {
      return 0;
    }
    String correlationId = null;
    if (callbackOnReply != null) {
      int totalTimeout = timeOut * (retryCount + 1);
      if (totalTimeout < 0) {
        totalTimeout = -1;
      }
      callbackOnReply.setTimeout(totalTimeout);
      correlationId = UUID.randomUUID().toString();
      for (List<Message> messages : generateMessage.values()) {
        callbackOnReply.setMessagesSent(messages);
      }
      _asyncCallbackService.registerAsyncCallback(correlationId, callbackOnReply);
    }

    HelixDataAccessor targetDataAccessor = getRecipientDataAccessor(recipientCriteria);
    for (InstanceType receiverType : generateMessage.keySet()) {
      List<Message> list = generateMessage.get(receiverType);
      for (Message tempMessage : list) {
        tempMessage.setRetryCount(retryCount);
        tempMessage.setExecutionTimeout(timeOut);
        tempMessage.setSrcInstanceType(_manager.getInstanceType());
        if (correlationId != null) {
          tempMessage.setCorrelationId(correlationId);
        }
        tempMessage.setSrcClusterName(_manager.getClusterName());

        Builder keyBuilder = targetDataAccessor.keyBuilder();
        if (receiverType == InstanceType.CONTROLLER) {
          targetDataAccessor
              .setProperty(keyBuilder.controllerMessage(tempMessage.getId()), tempMessage);
        } else if (receiverType == InstanceType.PARTICIPANT) {
          targetDataAccessor
              .setProperty(keyBuilder.message(tempMessage.getTgtName(), tempMessage.getId()),
                  tempMessage);
        }
      }
    }

      if (callbackOnReply != null) {
        // start timer if timeout is set
        callbackOnReply.startTimer();
      }
      return totalMessageCount;
  }

  private HelixDataAccessor getRecipientDataAccessor(final Criteria recipientCriteria) {
    HelixDataAccessor dataAccessor = _manager.getHelixDataAccessor();
    String clusterName = recipientCriteria.getClusterName();
    if (clusterName != null && !clusterName.equals(_manager.getClusterName())) {
      // for cross cluster message, create new DataAccessor for sending message.
      /*
        TODO On frequent cross clsuter messaging request, keeping construct data accessor may cause
        performance issue. We should consider adding cache in this service or HelixManager. --JJ
       */
      dataAccessor = new ZKHelixDataAccessor(clusterName, dataAccessor.getBaseDataAccessor());
    }
    return dataAccessor;
  }

  public Map<InstanceType, List<Message>> generateMessage(final Criteria recipientCriteria,
      final Message message) {
    Map<InstanceType, List<Message>> messagesToSendMap = new HashMap<InstanceType, List<Message>>();
    InstanceType instanceType = recipientCriteria.getRecipientInstanceType();

    HelixDataAccessor targetDataAccessor = getRecipientDataAccessor(recipientCriteria);

      List<Message> messages = Collections.EMPTY_LIST;
      if (instanceType == InstanceType.CONTROLLER) {
        messages = generateMessagesForController(message);
      } else if (instanceType == InstanceType.PARTICIPANT) {
        messages =
            generateMessagesForParticipant(recipientCriteria, message, targetDataAccessor);
      }
      messagesToSendMap.put(instanceType, messages);
      return messagesToSendMap;
  }

  private List<Message> generateMessagesForParticipant(Criteria recipientCriteria, Message message,
      HelixDataAccessor targetDataAccessor) {
    List<Message> messages = new ArrayList<Message>();
    List<Map<String, String>> matchedList =
        _evaluator.evaluateCriteria(recipientCriteria, targetDataAccessor);

    if (!matchedList.isEmpty()) {
      Map<String, String> sessionIdMap = new HashMap<String, String>();
      if (recipientCriteria.isSessionSpecific()) {
        Builder keyBuilder = targetDataAccessor.keyBuilder();
        // For backward compatibility, allow partial read for the live instances.
        // Note that this may cause the pending message to be sent with null target session Id.
        List<LiveInstance> liveInstances =
            targetDataAccessor.getChildValues(keyBuilder.liveInstances(), false);

        for (LiveInstance liveInstance : liveInstances) {
          sessionIdMap.put(liveInstance.getInstanceName(), liveInstance.getEphemeralOwner());
        }
      }
      for (Map<String, String> map : matchedList) {
        String id = UUID.randomUUID().toString();
        Message newMessage = new Message(message.getRecord(), id);
        String srcInstanceName = _manager.getInstanceName();
        String tgtInstanceName = map.get("instanceName");
        // Don't send message to self
        if (recipientCriteria.isSelfExcluded() && srcInstanceName.equalsIgnoreCase(tgtInstanceName)) {
          continue;
        }
        newMessage.setSrcName(srcInstanceName);
        newMessage.setTgtName(tgtInstanceName);
        newMessage.setResourceName(map.get("resourceName"));
        newMessage.setPartitionName(map.get("partitionName"));
        if (recipientCriteria.isSessionSpecific()) {
          newMessage.setTgtSessionId(sessionIdMap.get(tgtInstanceName));
        }
        messages.add(newMessage);
      }
    }
    return messages;
  }

  private List<Message> generateMessagesForController(Message message) {
    List<Message> messages = new ArrayList<Message>();
    String id = (message.getMsgId() == null) ? UUID.randomUUID().toString() : message.getMsgId();
    Message newMessage = new Message(message.getRecord(), id);
    newMessage.setMsgId(id);
    newMessage.setSrcName(_manager.getInstanceName());
    newMessage.setTgtName(InstanceType.CONTROLLER.name());
    messages.add(newMessage);
    return messages;
  }

  @Override
  public synchronized void registerMessageHandlerFactory(String type,
      MessageHandlerFactory factory) {
    registerMessageHandlerFactory(Collections.singletonList(type), factory);
  }

  @Override
  public synchronized void registerMessageHandlerFactory(List<String> types,
      MessageHandlerFactory factory) {
    if (_manager.isConnected()) {
      for (String type : types) {
        registerMessageHandlerFactoryInternal(type, factory);
      }
    } else {
      for (String type : types) {
        _messageHandlerFactoriestobeAdded.put(type, factory);
      }
    }
  }

  public synchronized void onConnected() {
    for (String type : _messageHandlerFactoriestobeAdded.keySet()) {
      registerMessageHandlerFactoryInternal(type, _messageHandlerFactoriestobeAdded.get(type));
    }
    _messageHandlerFactoriestobeAdded.clear();
  }

  void registerMessageHandlerFactoryInternal(String type, MessageHandlerFactory factory) {
    _logger.info("registering msg factory for type " + type);
    int threadpoolSize = HelixTaskExecutor.DEFAULT_PARALLEL_TASKS;
    String threadpoolSizeStr = null;
    String key = type + "." + HelixTaskExecutor.MAX_THREADS;

    ConfigAccessor configAccessor = _manager.getConfigAccessor();
    if (configAccessor != null) {
      ConfigScope scope = null;

      // Read the participant config and cluster config for the per-message type thread pool size.
      // participant config will override the cluster config.

      if (_manager.getInstanceType() == InstanceType.PARTICIPANT
          || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
        scope =
            new ConfigScopeBuilder().forCluster(_manager.getClusterName())
                .forParticipant(_manager.getInstanceName()).build();
        threadpoolSizeStr = configAccessor.get(scope, key);
      }

      if (threadpoolSizeStr == null) {
        scope = new ConfigScopeBuilder().forCluster(_manager.getClusterName()).build();
        threadpoolSizeStr = configAccessor.get(scope, key);
      }
    }

    if (threadpoolSizeStr != null) {
      try {
        threadpoolSize = Integer.parseInt(threadpoolSizeStr);
        if (threadpoolSize <= 0) {
          threadpoolSize = 1;
        }
      } catch (Exception e) {
        _logger.error("", e);
      }
    }

    _taskExecutor.registerMessageHandlerFactory(type, factory, threadpoolSize);
    // Self-send a no-op message, so that the onMessage() call will be invoked
    // again, and
    // we have a chance to process the message that we received with the new
    // added MessageHandlerFactory
    // before the factory is added.
    sendNopMessageInternal();
  }

  @Deprecated
  public void sendNopMessage() {
    sendNopMessageInternal();
  }

  private void sendNopMessageInternal() {
    try {
      Message nopMsg = new Message(MessageType.NO_OP, UUID.randomUUID().toString());
      nopMsg.setSrcName(_manager.getInstanceName());

      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();

      if (_manager.getInstanceType() == InstanceType.CONTROLLER
          || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
        nopMsg.setTgtName(InstanceType.CONTROLLER.name());
        accessor.setProperty(keyBuilder.controllerMessage(nopMsg.getId()), nopMsg);
      }

      if (_manager.getInstanceType() == InstanceType.PARTICIPANT
          || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
        nopMsg.setTgtName(_manager.getInstanceName());
        accessor.setProperty(keyBuilder.message(nopMsg.getTgtName(), nopMsg.getId()), nopMsg);
      }
    } catch (Exception e) {
      _logger.error(e.toString());
    }
  }

  public HelixTaskExecutor getExecutor() {
    return _taskExecutor;
  }

  @Override
  // TODO if the manager is not Participant or Controller, no reply, so should fail immediately
  public int sendAndWait(Criteria recipientCriteria, Message message, AsyncCallback asyncCallback,
      int timeOut, int retryCount) {
    int messagesSent = send(recipientCriteria, message, asyncCallback, timeOut, retryCount);
    if (messagesSent > 0) {
      synchronized (asyncCallback) {
        while (!asyncCallback.isDone() && !asyncCallback.isTimedOut()) {
          try {
            asyncCallback.wait();
          } catch (InterruptedException e) {
            _logger.error(e.toString());
            asyncCallback.setInterrupted(true);
            break;
          }
        }
      }
    } else {
      _logger.warn("No messages sent. For Criteria:" + recipientCriteria);
    }
    return messagesSent;
  }

  @Override
  public int sendAndWait(Criteria recipientCriteria, Message message, AsyncCallback asyncCallback,
      int timeOut) {
    return sendAndWait(recipientCriteria, message, asyncCallback, timeOut, 0);
  }
}
