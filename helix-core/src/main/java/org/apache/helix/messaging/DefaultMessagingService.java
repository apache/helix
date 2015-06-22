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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.messaging.handling.AsyncCallbackService;
import org.apache.helix.messaging.handling.HelixTaskExecutor;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;

public class DefaultMessagingService implements ClusterMessagingService {
  private final HelixManager _manager;
  private final CriteriaEvaluator _evaluator;
  private final HelixTaskExecutor _taskExecutor;
  // TODO:rename to factory, this is not a service
  private final AsyncCallbackService _asyncCallbackService;
  private static Logger _logger = Logger.getLogger(DefaultMessagingService.class);
  ConcurrentHashMap<String, MessageHandlerFactory> _messageHandlerFactoriestobeAdded =
      new ConcurrentHashMap<String, MessageHandlerFactory>();

  public DefaultMessagingService(HelixManager manager) {
    _manager = manager;
    _evaluator = new CriteriaEvaluator();
    _taskExecutor = new HelixTaskExecutor();
    _asyncCallbackService = new AsyncCallbackService();
    _taskExecutor.registerMessageHandlerFactory(MessageType.TASK_REPLY.toString(),
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

    for (InstanceType receiverType : generateMessage.keySet()) {
      List<Message> list = generateMessage.get(receiverType);
      for (Message tempMessage : list) {
        tempMessage.setRetryCount(retryCount);
        tempMessage.setExecutionTimeout(timeOut);
        tempMessage.setSrcInstanceType(_manager.getInstanceType());
        if (correlationId != null) {
          tempMessage.setCorrelationId(correlationId);
        }

        HelixDataAccessor accessor = _manager.getHelixDataAccessor();
        Builder keyBuilder = accessor.keyBuilder();

        if (receiverType == InstanceType.CONTROLLER) {
          accessor.setProperty(keyBuilder.controllerMessage(tempMessage.getId()), tempMessage);
        }

        if (receiverType == InstanceType.PARTICIPANT) {
          accessor.setProperty(keyBuilder.message(tempMessage.getTgtName(), tempMessage.getId()),
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

  @Override
  public Map<InstanceType, List<Message>> generateMessage(final Criteria recipientCriteria,
      final Message message) {
    Map<InstanceType, List<Message>> messagesToSendMap = new HashMap<InstanceType, List<Message>>();
    InstanceType instanceType = recipientCriteria.getRecipientInstanceType();

    if (instanceType == InstanceType.CONTROLLER) {
      List<Message> messages = generateMessagesForController(message);
      messagesToSendMap.put(InstanceType.CONTROLLER, messages);
      // _dataAccessor.setControllerProperty(PropertyType.MESSAGES,
      // newMessage.getRecord(), CreateMode.PERSISTENT);
    } else if (instanceType == InstanceType.PARTICIPANT) {
      List<Message> messages = new ArrayList<Message>();
      List<Map<String, String>> matchedList =
          _evaluator.evaluateCriteria(recipientCriteria, _manager);

      if (!matchedList.isEmpty()) {
        Map<String, String> sessionIdMap = new HashMap<String, String>();
        if (recipientCriteria.isSessionSpecific()) {
          HelixDataAccessor accessor = _manager.getHelixDataAccessor();
          Builder keyBuilder = accessor.keyBuilder();

          List<LiveInstance> liveInstances = accessor.getChildValues(keyBuilder.liveInstances());

          for (LiveInstance liveInstance : liveInstances) {
            sessionIdMap.put(liveInstance.getInstanceName(), liveInstance.getTypedSessionId()
                .stringify());
          }
        }
        for (Map<String, String> map : matchedList) {
          MessageId id = MessageId.from(UUID.randomUUID().toString());
          Message newMessage = new Message(message.getRecord(), id);
          String srcInstanceName = _manager.getInstanceName();
          String tgtInstanceName = map.get("instanceName");
          // Don't send message to self
          if (recipientCriteria.isSelfExcluded()
              && srcInstanceName.equalsIgnoreCase(tgtInstanceName)) {
            continue;
          }
          newMessage.setSrcName(srcInstanceName);
          newMessage.setTgtName(tgtInstanceName);
          newMessage.setResourceId(ResourceId.from(map.get("resourceName")));
          newMessage.setPartitionId(PartitionId.from(map.get("partitionName")));
          if (recipientCriteria.isSessionSpecific()) {
            newMessage.setTgtSessionId(SessionId.from(sessionIdMap.get(tgtInstanceName)));
          }
          messages.add(newMessage);
        }
        messagesToSendMap.put(InstanceType.PARTICIPANT, messages);
      }
    }
    return messagesToSendMap;
  }

  private List<Message> generateMessagesForController(Message message) {
    List<Message> messages = new ArrayList<Message>();
    MessageId id = MessageId.from(UUID.randomUUID().toString());
    Message newMessage = new Message(message.getRecord(), id);
    newMessage.setMessageId(id);
    newMessage.setSrcName(_manager.getInstanceName());
    newMessage.setTgtName("Controller");
    messages.add(newMessage);
    return messages;
  }

  @Override
  public synchronized void registerMessageHandlerFactory(String type, MessageHandlerFactory factory) {
    if (_manager.isConnected()) {
      registerMessageHandlerFactoryInternal(type, factory);
    } else {
      _messageHandlerFactoriestobeAdded.put(type, factory);
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
      HelixConfigScope scope = null;

      // Read the participant config and cluster config for the per-message type thread pool size.
      // participant config will override the cluster config.

      if (_manager.getInstanceType() == InstanceType.PARTICIPANT
          || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
        scope =
            new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT)
                .forCluster(_manager.getClusterName()).forParticipant(_manager.getInstanceName())
                .build();
        threadpoolSizeStr = configAccessor.get(scope, key);
      }

      if (threadpoolSizeStr == null) {
        scope =
            new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(
                _manager.getClusterName()).build();
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
      Message nopMsg = new Message(MessageType.NO_OP, MessageId.from(UUID.randomUUID().toString()));
      nopMsg.setSrcName(_manager.getInstanceName());

      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();

      if (_manager.getInstanceType() == InstanceType.CONTROLLER
          || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
        nopMsg.setTgtName("Controller");
        accessor.setProperty(keyBuilder.controllerMessage(nopMsg.getId()), nopMsg);
      }

      if (_manager.getInstanceType() == InstanceType.PARTICIPANT
          || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
        nopMsg.setTgtName(_manager.getInstanceName());
        accessor.setProperty(keyBuilder.message(nopMsg.getTgtName(), nopMsg.getId()), nopMsg);
      }
    } catch (Exception e) {
      _logger.error(e);
    }
  }

  public HelixTaskExecutor getExecutor() {
    return _taskExecutor;
  }

  @Override
  public int sendAndWait(Criteria receipientCriteria, Message message, AsyncCallback asyncCallback,
      int timeOut, int retryCount) {
    int messagesSent = send(receipientCriteria, message, asyncCallback, timeOut, retryCount);
    if (messagesSent > 0) {
      while (!asyncCallback.isDone() && !asyncCallback.isTimedOut()) {
        synchronized (asyncCallback) {
          try {
            asyncCallback.wait();
          } catch (InterruptedException e) {
            _logger.error(e);
            asyncCallback.setInterrupted(true);
            break;
          }
        }
      }
    } else {
      _logger.warn("No messages sent. For Criteria:" + receipientCriteria);
    }
    return messagesSent;
  }

  @Override
  public int sendAndWait(Criteria recipientCriteria, Message message, AsyncCallback asyncCallback,
      int timeOut) {
    return sendAndWait(recipientCriteria, message, asyncCallback, timeOut, 0);
  }
}
