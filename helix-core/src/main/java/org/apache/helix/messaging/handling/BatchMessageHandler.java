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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.MapKey;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.Attributes;
import org.apache.log4j.Logger;

public class BatchMessageHandler extends MessageHandler {
  private static Logger LOG = Logger.getLogger(BatchMessageHandler.class);

  final MessageHandlerFactory _msgHandlerFty;
  final TaskExecutor _executor;
  final List<Message> _subMessages;
  final List<MessageHandler> _subMessageHandlers;
  final BatchMessageWrapper _batchMsgWrapper;

  public BatchMessageHandler(Message msg, NotificationContext context, MessageHandlerFactory fty,
      BatchMessageWrapper wrapper, TaskExecutor executor) {
    super(msg, context);

    if (fty == null || executor == null) {
      throw new HelixException("MessageHandlerFactory | TaskExecutor can't be null");
    }

    _msgHandlerFty = fty;
    _batchMsgWrapper = wrapper;
    _executor = executor;

    // create sub-messages
    _subMessages = new ArrayList<Message>();
    List<PartitionId> partitionKeys = _message.getPartitionIds();
    for (PartitionId partitionKey : partitionKeys) {
      // assign a new message id, put batch-msg-id to parent-id field
      Message subMsg =
          new Message(_message.getRecord(), MessageId.from(UUID.randomUUID().toString()));
      subMsg.setPartitionId(partitionKey);
      subMsg.setAttribute(Attributes.PARENT_MSG_ID, _message.getId());
      subMsg.setBatchMessageMode(false);

      _subMessages.add(subMsg);
    }

    // create sub-message handlers
    _subMessageHandlers = createMsgHandlers(_subMessages, context);
  }

  List<MessageHandler> createMsgHandlers(List<Message> msgs, NotificationContext context) {

    List<MessageHandler> handlers = new ArrayList<MessageHandler>();
    for (Message msg : msgs) {
      MessageHandler handler = _msgHandlerFty.createHandler(msg, context);
      handlers.add(handler);
    }
    return handlers;
  }

  public void preHandleMessage() {
    if (_message.getBatchMessageMode() == true && _batchMsgWrapper != null) {
      _batchMsgWrapper.start(_message, _notificationContext);
    }
  }

  public void postHandleMessage() {
    if (_message.getBatchMessageMode() == true && _batchMsgWrapper != null) {
      _batchMsgWrapper.end(_message, _notificationContext);
    }

    // update currentState
    HelixManager manager = _notificationContext.getManager();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    ConcurrentHashMap<String, CurrentStateUpdate> csUpdateMap =
        (ConcurrentHashMap<String, CurrentStateUpdate>) _notificationContext
            .get(MapKey.CURRENT_STATE_UPDATE.toString());

    if (csUpdateMap != null) {
      Map<PropertyKey, CurrentState> csUpdate = mergeCurStateUpdate(csUpdateMap);

      // TODO: change to use asyncSet
      for (PropertyKey key : csUpdate.keySet()) {
        // logger.info("updateCS: " + key);
        // System.out.println("\tupdateCS: " + key.getPath() + ", " +
        // curStateMap.get(key));
        accessor.updateProperty(key, csUpdate.get(key));
      }
    }
  }

  // will not return until all sub-message executions are done
  @Override
  public HelixTaskResult handleMessage() {
    HelixTaskResult result = null;
    List<Future<HelixTaskResult>> futures = null;
    List<MessageTask> batchTasks = new ArrayList<MessageTask>();

    synchronized (_batchMsgWrapper) {
      try {
        preHandleMessage();

        int exeBatchSize = 1; // TODO: getExeBatchSize from msg
        List<PartitionId> partitionKeys = _message.getPartitionIds();
        for (int i = 0; i < partitionKeys.size(); i += exeBatchSize) {
          if (i + exeBatchSize <= partitionKeys.size()) {
            List<Message> msgs = _subMessages.subList(i, i + exeBatchSize);
            List<MessageHandler> handlers = _subMessageHandlers.subList(i, i + exeBatchSize);
            HelixBatchMessageTask batchTask =
                new HelixBatchMessageTask(_message, msgs, handlers, _notificationContext);
            batchTasks.add(batchTask);

          } else {
            List<Message> msgs = _subMessages.subList(i, i + partitionKeys.size());
            List<MessageHandler> handlers =
                _subMessageHandlers.subList(i, i + partitionKeys.size());

            HelixBatchMessageTask batchTask =
                new HelixBatchMessageTask(_message, msgs, handlers, _notificationContext);
            batchTasks.add(batchTask);
          }
        }

        // invokeAll() is blocking call
        long timeout = _message.getExecutionTimeout();
        if (timeout == -1) {
          timeout = Long.MAX_VALUE;
        }
        futures = _executor.invokeAllTasks(batchTasks, timeout, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        LOG.error("fail to execute batchMsg: " + _message.getId(), e);
        result = new HelixTaskResult();
        result.setException(e);

        // HelixTask will call onError on this batch-msg-handler
        // return result;
      }

      // combine sub-results to result
      if (futures != null) {
        boolean isBatchTaskSucceed = true;

        for (int i = 0; i < futures.size(); i++) {
          Future<HelixTaskResult> future = futures.get(i);
          MessageTask subTask = batchTasks.get(i);
          try {
            HelixTaskResult subTaskResult = future.get();
            if (!subTaskResult.isSuccess()) {
              isBatchTaskSucceed = false;
            }
          } catch (InterruptedException e) {
            isBatchTaskSucceed = false;
            LOG.error("interrupted in executing batch-msg: " + _message.getId() + ", sub-msg: "
                + subTask.getTaskId(), e);
          } catch (ExecutionException e) {
            isBatchTaskSucceed = false;
            LOG.error(
                "fail to execute batch-msg: " + _message.getId() + ", sub-msg: "
                    + subTask.getTaskId(), e);
          }
        }
        result = new HelixTaskResult();
        result.setSuccess(isBatchTaskSucceed);
      }

      // pass task-result to post-handle-msg
      _notificationContext.add(MapKey.HELIX_TASK_RESULT.toString(), result);
      postHandleMessage();

      return result;
    }
  }

  @Override
  public void onError(Exception e, ErrorCode code, ErrorType type) {
    // if one sub-message execution fails, call onError on all sub-message handlers
    for (MessageHandler handler : _subMessageHandlers) {
      handler.onError(e, code, type);
    }
  }

  // TODO: optimize this based on the fact that each cs update is for a
  // distinct partition
  private Map<PropertyKey, CurrentState> mergeCurStateUpdate(
      ConcurrentHashMap<String, CurrentStateUpdate> csUpdateMap) {
    Map<String, CurrentStateUpdate> curStateUpdateMap = new HashMap<String, CurrentStateUpdate>();
    for (CurrentStateUpdate update : csUpdateMap.values()) {
      String path = update._key.getPath(); // TODO: this is time
                                           // consuming, optimize it
      if (!curStateUpdateMap.containsKey(path)) {
        curStateUpdateMap.put(path, update);
      } else {
        // long start = System.currentTimeMillis();
        curStateUpdateMap.get(path).merge(update._delta);
        // long end = System.currentTimeMillis();
        // LOG.info("each merge took: " + (end - start));
      }
    }

    Map<PropertyKey, CurrentState> ret = new HashMap<PropertyKey, CurrentState>();
    for (CurrentStateUpdate update : curStateUpdateMap.values()) {
      ret.put(update._key, update._delta);
    }

    return ret;
  }

}
