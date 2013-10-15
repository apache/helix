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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.MapKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.State;
import org.apache.helix.messaging.handling.MessageHandler.ErrorCode;
import org.apache.helix.messaging.handling.MessageHandler.ErrorType;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.Attributes;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.monitoring.StateTransitionContext;
import org.apache.helix.monitoring.StateTransitionDataPoint;
import org.apache.helix.util.StatusUpdateUtil;
import org.apache.log4j.Logger;

public class HelixTask implements MessageTask {
  private static Logger logger = Logger.getLogger(HelixTask.class);
  private final Message _message;
  private final MessageHandler _handler;
  private final NotificationContext _notificationContext;
  private final HelixManager _manager;
  StatusUpdateUtil _statusUpdateUtil;
  HelixTaskExecutor _executor;
  volatile boolean _isTimeout = false;

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

    long start = System.currentTimeMillis();
    logger.info("handling task: " + getTaskId() + " begin, at: " + start);
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    _statusUpdateUtil.logInfo(_message, HelixTask.class, "Message handling task begin execute",
        accessor);
    _message.setExecuteStartTimeStamp(new Date().getTime());

    // add a concurrent map to hold currentStateUpdates for sub-messages of a batch-message
    // partitionName -> csUpdate
    if (_message.getBatchMessageMode() == true) {
      _notificationContext.add(MapKey.CURRENT_STATE_UPDATE.toString(),
          new ConcurrentHashMap<String, CurrentStateUpdate>());
    }

    // Handle the message
    try {
      taskResult = _handler.handleMessage();
    } catch (InterruptedException e) {
      taskResult = new HelixTaskResult();
      taskResult.setException(e);
      taskResult.setInterrupted(true);

      _statusUpdateUtil.logError(_message, HelixTask.class, e,
          "State transition interrupted, timeout:" + _isTimeout, accessor);
      logger.info("Message " + _message.getMessageId() + " is interrupted");
    } catch (Exception e) {
      taskResult = new HelixTaskResult();
      taskResult.setException(e);
      taskResult.setMessage(e.getMessage());

      String errorMessage =
          "Exception while executing a message. " + e + " msgId: " + _message.getMessageId()
              + " type: " + _message.getMsgType();
      logger.error(errorMessage, e);
      _statusUpdateUtil.logError(_message, HelixTask.class, e, errorMessage, accessor);
    }

    // cancel timeout task
    _executor.cancelTimeoutTask(this);

    Exception exception = null;
    try {
      if (taskResult.isSuccess()) {
        _statusUpdateUtil.logInfo(_message, _handler.getClass(),
            "Message handling task completed successfully", accessor);
        logger.info("Message " + _message.getMessageId() + " completed.");
      } else {
        type = ErrorType.INTERNAL;

        if (taskResult.isInterrupted()) {
          logger.info("Message " + _message.getMessageId() + " is interrupted");
          code = _isTimeout ? ErrorCode.TIMEOUT : ErrorCode.CANCEL;
          if (_isTimeout) {
            int retryCount = _message.getRetryCount();
            logger.info("Message timeout, retry count: " + retryCount + " msgId:"
                + _message.getMessageId());
            _statusUpdateUtil.logInfo(_message, _handler.getClass(),
                "Message handling task timeout, retryCount:" + retryCount, accessor);
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
        } else // logging for errors
        {
          code = ErrorCode.ERROR;
          String errorMsg =
              "Message execution failed. msgId: " + getTaskId() + ", errorMsg: "
                  + taskResult.getMessage();
          logger.error(errorMsg);
          _statusUpdateUtil.logError(_message, _handler.getClass(), errorMsg, accessor);
        }
      }

      if (_message.getAttribute(Attributes.PARENT_MSG_ID) == null) {
        // System.err.println("\t[dbg]remove msg: " + getTaskId());
        removeMessageFromZk(accessor, _message);
        reportMessageStat(_manager, _message, taskResult);
        sendReply(accessor, _message, taskResult);
        _executor.finishTask(this);
      }
    } catch (Exception e) {
      exception = e;
      type = ErrorType.FRAMEWORK;
      code = ErrorCode.ERROR;

      String errorMessage =
          "Exception after executing a message, msgId: " + _message.getMessageId() + e;
      logger.error(errorMessage, e);
      _statusUpdateUtil.logError(_message, HelixTask.class, errorMessage, accessor);
    } finally {
      long end = System.currentTimeMillis();
      logger.info("msg: " + _message.getMessageId() + " handling task completed, results:"
          + taskResult.isSuccess() + ", at: " + end + ", took:" + (end - start));

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
    Builder keyBuilder = accessor.keyBuilder();
    if (message.getTgtName().equalsIgnoreCase("controller")) {
      // TODO: removeProperty returns boolean
      accessor.removeProperty(keyBuilder.controllerMessage(message.getMessageId().stringify()));
    } else {
      accessor.removeProperty(keyBuilder.message(_manager.getInstanceName(), message.getMessageId()
          .stringify()));
    }
  }

  private void sendReply(HelixDataAccessor accessor, Message message, HelixTaskResult taskResult) {
    if (_message.getCorrelationId() != null
        && !message.getMsgType().equals(MessageType.TASK_REPLY.toString())) {
      logger.info("Sending reply for message " + message.getCorrelationId());
      _statusUpdateUtil.logInfo(message, HelixTask.class, "Sending reply", accessor);

      taskResult.getTaskResultMap().put("SUCCESS", "" + taskResult.isSuccess());
      taskResult.getTaskResultMap().put("INTERRUPTED", "" + taskResult.isInterrupted());
      if (!taskResult.isSuccess()) {
        taskResult.getTaskResultMap().put("ERRORINFO", taskResult.getMessage());
      }
      Message replyMessage =
          Message.createReplyMessage(_message, _manager.getInstanceName(),
              taskResult.getTaskResultMap());
      replyMessage.setSrcInstanceType(_manager.getInstanceType());

      if (message.getSrcInstanceType() == InstanceType.PARTICIPANT) {
        Builder keyBuilder = accessor.keyBuilder();
        accessor.setProperty(
            keyBuilder.message(message.getMsgSrc(), replyMessage.getMessageId().stringify()),
            replyMessage);
      } else if (message.getSrcInstanceType() == InstanceType.CONTROLLER) {
        Builder keyBuilder = accessor.keyBuilder();
        accessor.setProperty(keyBuilder.controllerMessage(replyMessage.getMessageId().stringify()),
            replyMessage);
      }
      _statusUpdateUtil.logInfo(message, HelixTask.class,
          "1 msg replied to " + replyMessage.getTgtName(), accessor);
    }
  }

  private void reportMessageStat(HelixManager manager, Message message, HelixTaskResult taskResult) {
    // report stat
    if (!message.getMsgType().equals(MessageType.STATE_TRANSITION.toString())) {
      return;
    }
    long now = new Date().getTime();
    long msgReadTime = message.getReadTimeStamp();
    long msgExecutionStartTime = message.getExecuteStartTimeStamp();
    if (msgReadTime != 0 && msgExecutionStartTime != 0) {
      long totalDelay = now - msgReadTime;
      long executionDelay = now - msgExecutionStartTime;
      if (totalDelay > 0 && executionDelay > 0) {
        State fromState = message.getTypedFromState();
        State toState = message.getTypedToState();
        String transition = fromState + "--" + toState;

        StateTransitionContext cxt =
            new StateTransitionContext(manager.getClusterName(), manager.getInstanceName(), message
                .getResourceId().stringify(), transition);

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
};
