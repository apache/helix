package org.apache.helix.messaging.handling;

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

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixRollbackException;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.MapKey;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.messaging.handling.MessageHandler.ErrorCode;
import org.apache.helix.messaging.handling.MessageHandler.ErrorType;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.Attributes;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.monitoring.StateTransitionContext;
import org.apache.helix.monitoring.StateTransitionDataPoint;
import org.apache.helix.monitoring.mbeans.ParticipantMessageMonitor;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.StatusUpdateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelixTask implements MessageTask {
  private static Logger logger = LoggerFactory.getLogger(HelixTask.class);
  private final Message _message;
  private final MessageHandler _handler;
  private final NotificationContext _notificationContext;
  private final HelixManager _manager;
  StatusUpdateUtil _statusUpdateUtil;
  HelixTaskExecutor _executor;
  volatile boolean _isTimeout = false;
  volatile boolean _isStarted = false;
  volatile boolean _isCancelled = false;

  public HelixTask(Message message, NotificationContext notificationContext,
      MessageHandler handler, HelixTaskExecutor executor) {
    this._notificationContext = notificationContext;
    this._message = message;
    this._handler = handler;
    this._manager = notificationContext.getManager();
    _statusUpdateUtil = new StatusUpdateUtil();
    _executor = executor;
  }

  @Override
  public HelixTaskResult call() {
    HelixTaskResult taskResult = null;

    ErrorType type = null;
    ErrorCode code = null;
    Long handlerStart = null;
    Long handlerEnd = null;

    long start = System.currentTimeMillis();
    logger.info("handling task: " + getTaskId() + " begin, at: " + start);
    _statusUpdateUtil.logInfo(_message, HelixTask.class, "Message handling task begin execute",
        _manager);
    _message.setExecuteStartTimeStamp(new Date().getTime());

    // add a concurrent map to hold currentStateUpdates for sub-messages of a batch-message
    // partitionName -> csUpdate
    if (_message.getBatchMessageMode()) {
      _notificationContext.add(MapKey.CURRENT_STATE_UPDATE.toString(),
          new ConcurrentHashMap<String, CurrentStateUpdate>());
    }

    // Handle the message
    try {
      setStarted();
      handlerStart = System.currentTimeMillis();
      taskResult = _handler.handleMessage();
      handlerEnd = System.currentTimeMillis();
    } catch (InterruptedException e) {
      taskResult = new HelixTaskResult();
      taskResult.setException(e);
      taskResult.setInterrupted(true);

      _statusUpdateUtil.logError(_message, HelixTask.class, e,
          "State transition interrupted, timeout:" + _isTimeout, _manager);
      logger.info("Message " + _message.getMsgId() + " is interrupted");
    } catch (Exception e) {
      taskResult = new HelixTaskResult();
      taskResult.setException(e);
      taskResult.setMessage(e.getMessage());

      String errorMessage =
          "Exception while executing a message. " + e + " msgId: " + _message.getMsgId()
              + " type: " + _message.getMsgType();
      logger.error(errorMessage, e);
      _statusUpdateUtil.logError(_message, HelixTask.class, e, errorMessage, _manager);
    }

    // cancel timeout task
    _executor.cancelTimeoutTask(this);

    Exception exception = null;
    try {
      if (taskResult.isSuccess()) {
        _statusUpdateUtil
            .logInfo(_message, _handler.getClass(), "Message handling task completed successfully", _manager);
        logger.info("Message " + _message.getMsgId() + " completed.");
        _executor.getParticipantMonitor().reportProcessedMessage(_message, ParticipantMessageMonitor.ProcessedMessageState.COMPLETED);
      } else {
        type = ErrorType.INTERNAL;

        if (taskResult.isInterrupted()) {
          logger.info("Message " + _message.getMsgId() + " is interrupted");
          code = _isTimeout ? ErrorCode.TIMEOUT : ErrorCode.CANCEL;
          if (_isTimeout) {
            int retryCount = _message.getRetryCount();
            logger.info("Message timeout, retry count: " + retryCount + " msgId:"
                + _message.getMsgId());
            _statusUpdateUtil.logInfo(_message, _handler.getClass(),
                "Message handling task timeout, retryCount:" + retryCount, _manager);
            // Notify the handler that timeout happens, and the number of retries left
            // In case timeout happens (time out and also interrupted)
            // we should retry the execution of the message by re-schedule it in
            if (retryCount > 0) {
              _message.setRetryCount(retryCount - 1);
              HelixTask task = new HelixTask(_message, _notificationContext, _handler, _executor);
              _executor.scheduleTask(task);
              return taskResult;
            }
          }
          _executor.getParticipantMonitor().reportProcessedMessage(
              _message, ParticipantMessageMonitor.ProcessedMessageState.DISCARDED);
        } else if (taskResult.isCancelled()) {
          type = null;
          _statusUpdateUtil
              .logInfo(_message, _handler.getClass(), "Cancellation completed successfully",
                  _manager);
          _executor.getParticipantMonitor().reportProcessedMessage(
              _message, ParticipantMessageMonitor.ProcessedMessageState.DISCARDED);
        } else {// logging for errors
          code = ErrorCode.ERROR;
          String errorMsg =
              "Message execution failed. msgId: " + getTaskId() + ", errorMsg: "
                  + taskResult.getMessage();
          logger.error(errorMsg);
          _statusUpdateUtil.logError(_message, _handler.getClass(), errorMsg, _manager);
          _executor.getParticipantMonitor().reportProcessedMessage(
              _message, ParticipantMessageMonitor.ProcessedMessageState.FAILED);
        }
      }

      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      // forward relay messages attached to this message to other participants
      if (taskResult.isSuccess()) {
        try {
          forwardRelayMessages(accessor, _message, taskResult.getCompleteTime());
        } catch (Exception e) {
          // Fail to send relay message should not result in a task execution failure
          // Currently we don't log error to ZK to reduce writes as when accessor throws
          // exception, ZK might not be in good condition.
          logger.warn("Failed to send relay messages.", e);
        }
      }

      if (_message.getAttribute(Attributes.PARENT_MSG_ID) == null) {
        removeMessageFromZk(accessor, _message);
        reportMessageStat(_manager, _message, taskResult);
        sendReply(getSrcClusterDataAccessor(_message), _message, taskResult);
        _executor.finishTask(this);
      }
    } catch (Exception e) {
      exception = e;
      type = ErrorType.FRAMEWORK;
      code = ErrorCode.ERROR;

      String errorMessage =
          "Exception after executing a message, msgId: " + _message.getMsgId() + e;
      logger.error(errorMessage, e);
      _statusUpdateUtil.logError(_message, HelixTask.class, errorMessage, _manager);
    } finally {
      long end = System.currentTimeMillis();
      long totalDuration = end - start;
      long handlerDuration =
          handlerStart != null && handlerEnd != null ? handlerEnd - handlerStart : -1;
      logger.info(
          "Message: {} (parent: {}) handling task for {}:{} completed at: {}, results: {}. FrameworkTime: {} ms; HandlerTime: {} ms.",
          _message.getMsgId(), _message.getAttribute(Attributes.PARENT_MSG_ID), _message.getResourceName(),
          _message.getPartitionName(), end, taskResult.isSuccess(), totalDuration - handlerDuration,
          handlerDuration);

      // Notify the handler about any error happened in the handling procedure, so that
      // the handler have chance to finally cleanup
      if (type == ErrorType.INTERNAL) {
        _handler.onError(taskResult.getException(), code, type);
      } else if (type == ErrorType.FRAMEWORK) {
        _handler.onError(exception, code, type);
      }
    }

    return taskResult;
  }

  private void removeMessageFromZk(HelixDataAccessor accessor, Message message) {
    if (!HelixUtil.removeMessageFromZK(accessor, message, _manager.getInstanceName())) {
      logger.warn("Failed to delete message " + message.getId() + " from zk!");
    } else {
      logger.info("Delete message " + message.getId() + " from zk!");
    }
  }

  private void forwardRelayMessages(HelixDataAccessor accessor, Message message,
      long taskCompletionTime) {
    if (message.hasRelayMessages()) {
      Map<String, Message> relayMessages = message.getRelayMessages();
      Builder keyBuilder = accessor.keyBuilder();

      // Ignore all relay messages if participant's session has changed.
      if (!_manager.getSessionId().equals(message.getTgtSessionId())) {
        logger.info(
            "Session id has been changed, ignore all relay messages attached with " + message
                .getId());
        return;
      }

      for (String instance : relayMessages.keySet()) {
        Message msg = relayMessages.get(instance);
        if (msg.getMsgSubType().equals(MessageType.RELAYED_MESSAGE.name())) {
          msg.setRelayTime(taskCompletionTime);
          if (msg.isExpired()) {
            logger.info(
                "Relay message expired, ignore " + msg.getId() + " to instance " + instance);
            continue;
          }
          PropertyKey msgKey = keyBuilder.message(instance, msg.getId());
          boolean success = accessor.getBaseDataAccessor()
              .create(msgKey.getPath(), msg.getRecord(), AccessOption.PERSISTENT);
          if (!success) {
            logger.warn("Failed to send relay message " + msg.getId() + " to " + instance);
          } else {
            logger.info("Send relay message " + msg.getId() + " to " + instance);
          }
        }
      }
    }
  }

  private HelixDataAccessor getSrcClusterDataAccessor(final Message message) {
    HelixDataAccessor helixDataAccessor = _manager.getHelixDataAccessor();
    String clusterName = message.getSrcClusterName();
    if (clusterName != null && !clusterName.equals(_manager.getClusterName())) {
      // for cross cluster message, create different HelixDataAccessor for replying message.
      /*
        TODO On frequent cross clsuter messaging request, keeping construct data accessor may cause
        performance issue. We should consider adding cache in this class or HelixManager. --JJ
       */
      helixDataAccessor = new ZKHelixDataAccessor(clusterName, helixDataAccessor.getBaseDataAccessor());
    }
    return helixDataAccessor;
  }

  private void sendReply(HelixDataAccessor replyDataAccessor, Message message,
      HelixTaskResult taskResult) {
    if (message.getCorrelationId() != null && !message.getMsgType()
        .equals(MessageType.TASK_REPLY.name())) {
      logger.info("Sending reply for message " + message.getCorrelationId());
      _statusUpdateUtil.logInfo(message, HelixTask.class, "Sending reply", _manager);

      taskResult.getTaskResultMap().put("SUCCESS", "" + taskResult.isSuccess());
      taskResult.getTaskResultMap().put("INTERRUPTED", "" + taskResult.isInterrupted());
      if (!taskResult.isSuccess()) {
        taskResult.getTaskResultMap().put("ERRORINFO", taskResult.getMessage());
      }
      Message replyMessage = Message
          .createReplyMessage(message, _manager.getInstanceName(), taskResult.getTaskResultMap());
      replyMessage.setSrcInstanceType(_manager.getInstanceType());

      Builder keyBuilder = replyDataAccessor.keyBuilder();
      if (message.getSrcInstanceType() == InstanceType.PARTICIPANT) {
        replyDataAccessor
            .setProperty(keyBuilder.message(message.getMsgSrc(), replyMessage.getMsgId()),
                replyMessage);
      } else if (message.getSrcInstanceType() == InstanceType.CONTROLLER) {
        replyDataAccessor
            .setProperty(keyBuilder.controllerMessage(replyMessage.getMsgId()), replyMessage);
      }
      _statusUpdateUtil.logInfo(message, HelixTask.class, String
          .format("1 msg replied to %s in cluster %s.", replyMessage.getTgtName(),
              message.getSrcClusterName() == null ?
                  _manager.getClusterName() :
                  message.getSrcClusterName()), _manager);
    }
  }

  private void reportMessageStat(HelixManager manager, Message message, HelixTaskResult taskResult) {
    // report stat
    if (!message.getMsgType().equals(MessageType.STATE_TRANSITION.name())) {
      return;
    }
    long now = new Date().getTime();
    long msgReadTime = message.getReadTimeStamp();
    long msgExecutionStartTime = message.getExecuteStartTimeStamp();
    if (msgReadTime != 0 && msgExecutionStartTime != 0) {
      long totalDelay = now - msgReadTime;
      long executionDelay = now - msgExecutionStartTime;
      if (totalDelay > 0 && executionDelay > 0) {
        String fromState = message.getFromState();
        String toState = message.getToState();
        String transition = fromState + "--" + toState;

        StateTransitionContext cxt =
            new StateTransitionContext(manager.getClusterName(), manager.getInstanceName(),
                message.getResourceName(), transition);

        StateTransitionDataPoint data =
            new StateTransitionDataPoint(totalDelay, executionDelay, taskResult.isSuccess());
        _executor.getParticipantMonitor().reportTransitionStat(cxt, data);
      }
    } else {
      logger.warn("message read time and start execution time not recorded.");
    }
  }

  @Override
  public String getTaskId() {
    return _message.getId();
  }

  @Override
  public Message getMessage() {
    return _message;
  }

  @Override
  public NotificationContext getNotificationContext() {
    return _notificationContext;
  }

  @Override
  public void onTimeout() {
    _isTimeout = true;
    _handler.onTimeout();
  }

  @Override
  public synchronized boolean cancel() {
    if (!_isStarted) {
      _isCancelled = true;
      _handler.cancel();
      return true;
    }
    return false;
  }

  private synchronized void setStarted() {
    if (_isCancelled) {
      throw new HelixRollbackException("Task has already been cancelled");
    }
    _isStarted = true;
  }
}
