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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixException;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.log4j.Logger;

public class AsyncCallbackService implements MessageHandlerFactory {
  private final ConcurrentHashMap<String, AsyncCallback> _callbackMap =
      new ConcurrentHashMap<String, AsyncCallback>();
  private static Logger _logger = Logger.getLogger(AsyncCallbackService.class);

  public AsyncCallbackService() {
  }

  public void registerAsyncCallback(String correlationId, AsyncCallback callback) {
    if (_callbackMap.containsKey(correlationId)) {
      _logger.warn("correlation id " + correlationId + " already registered");
    }
    _logger.info("registering correlation id " + correlationId);
    _callbackMap.put(correlationId, callback);
  }

  void verifyMessage(Message message) {
    if (!message.getMsgType().toString().equalsIgnoreCase(MessageType.TASK_REPLY.toString())) {
      String errorMsg =
          "Unexpected msg type for message " + message.getMessageId() + " type:"
              + message.getMsgType() + " Expected : " + MessageType.TASK_REPLY;
      _logger.error(errorMsg);
      throw new HelixException(errorMsg);
    }
    String correlationId = message.getCorrelationId();
    if (correlationId == null) {
      String errorMsg = "Message " + message.getMessageId() + " does not have correlation id";
      _logger.error(errorMsg);
      throw new HelixException(errorMsg);
    }

    if (!_callbackMap.containsKey(correlationId)) {
      String errorMsg =
          "Message "
              + message.getMessageId()
              + " does not have correponding callback. Probably timed out already. Correlation id: "
              + correlationId;
      _logger.error(errorMsg);
      throw new HelixException(errorMsg);
    }
    _logger.info("Verified reply message " + message.getMessageId() + " correlation:"
        + correlationId);
  }

  @Override
  public MessageHandler createHandler(Message message, NotificationContext context) {
    verifyMessage(message);
    return new AsyncCallbackMessageHandler(message.getCorrelationId(), message, context);
  }

  @Override
  public String getMessageType() {
    return MessageType.TASK_REPLY.toString();
  }

  @Override
  public void reset() {

  }

  public class AsyncCallbackMessageHandler extends MessageHandler {
    private final String _correlationId;

    public AsyncCallbackMessageHandler(String correlationId, Message message,
        NotificationContext context) {
      super(message, context);
      _correlationId = correlationId;
    }

    @Override
    public HelixTaskResult handleMessage() throws InterruptedException {
      verifyMessage(_message);
      HelixTaskResult result = new HelixTaskResult();
      assert (_correlationId.equalsIgnoreCase(_message.getCorrelationId()));
      _logger.info("invoking reply message " + _message.getMessageId() + ", correlationid:"
          + _correlationId);

      AsyncCallback callback = _callbackMap.get(_correlationId);
      synchronized (callback) {
        callback.onReply(_message);
        if (callback.isDone()) {
          _logger.info("Removing finished callback, correlationid:" + _correlationId);
          _callbackMap.remove(_correlationId);
        }
      }
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      _logger.error("Message handling pipeline get an exception. MsgId:" + _message.getMessageId(),
          e);
    }
  }
}
