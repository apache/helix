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
import java.util.List;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.log4j.Logger;

public class HelixBatchMessageTask implements MessageTask {
  private static Logger LOG = Logger.getLogger(HelixBatchMessageTask.class);

  final NotificationContext _context;
  final Message _batchMsg;
  final List<Message> _subMsgs;
  final List<MessageHandler> _handlers;

  public HelixBatchMessageTask(Message batchMsg, List<Message> subMsgs,
      List<MessageHandler> handlers, NotificationContext context) {
    _batchMsg = batchMsg;
    _context = context;
    _subMsgs = subMsgs;
    _handlers = handlers;
  }

  @Override
  public HelixTaskResult call() throws Exception {
    HelixTaskResult taskResult = null;

    long start = System.currentTimeMillis();
    LOG.info("taskId:" + getTaskId() + " handling task begin, at: " + start);

    boolean isSucceed = true;
    try {
      for (MessageHandler handler : _handlers) {
        if (handler != null) {
          HelixTaskResult subTaskResult = handler.handleMessage();
          // if any fails, return false
          if (!subTaskResult.isSuccess()) {
            // System.err.println("\t[dbg]error handling message: " + handler._message);
            isSucceed = false;
          }
        }
      }
    } catch (Exception e) {
      String errorMessage = "Exception while executing task: " + getTaskId();
      LOG.error(errorMessage, e);

      taskResult = new HelixTaskResult();
      taskResult.setException(e);
      taskResult.setMessage(e.getMessage());

      return taskResult;
    }

    if (isSucceed) {
      LOG.info("task: " + getTaskId() + " completed sucessfully");
    }

    taskResult = new HelixTaskResult();
    taskResult.setSuccess(isSucceed);
    return taskResult;
  }

  @Override
  public String getTaskId() {
    StringBuilder sb = new StringBuilder();
    sb.append(_batchMsg.getId());
    sb.append("/");
    List<String> msgIdList = new ArrayList<String>();
    if (_subMsgs != null) {
      for (Message msg : _subMsgs) {
        msgIdList.add(msg.getId());
      }
    }
    sb.append(msgIdList);
    return sb.toString();
  }

  @Override
  public Message getMessage() {
    return _batchMsg;
  }

  @Override
  public NotificationContext getNotificationContext() {
    return _context;
  }

  @Override
  public void onTimeout() {
    for (MessageHandler handler : _handlers) {
      if (handler != null) {
        handler.onTimeout();
      }
    }
  }
}
