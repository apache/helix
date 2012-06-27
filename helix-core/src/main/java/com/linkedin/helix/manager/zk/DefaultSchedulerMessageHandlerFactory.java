/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.manager.zk;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.helix.Criteria;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.messaging.AsyncCallback;
import com.linkedin.helix.messaging.handling.HelixTaskResult;
import com.linkedin.helix.messaging.handling.MessageHandler;
import com.linkedin.helix.messaging.handling.MessageHandlerFactory;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.model.StatusUpdate;
import com.linkedin.helix.util.StatusUpdateUtil;

/*
 * TODO: The current implementation is temporary for backup handler testing only and it does not 
 * do any throttling. 
 * 
 * **/
public class DefaultSchedulerMessageHandlerFactory implements MessageHandlerFactory
{
  public static class SchedulerAsyncCallback extends AsyncCallback
  {
    StatusUpdateUtil                       _statusUpdateUtil = new StatusUpdateUtil();
    Message                                _originalMessage;
    HelixManager                           _manager;
    final Map<String, Map<String, String>> _resultSummaryMap =
                                                                 new ConcurrentHashMap<String, Map<String, String>>();

    public SchedulerAsyncCallback(Message originalMessage, HelixManager manager)
    {
      _originalMessage = originalMessage;
      _manager = manager;
    }

    @Override
    public void onTimeOut()
    {
      _logger.info("Scheduler msg timeout " + _originalMessage.getMsgId()
          + " timout with " + _timeout + " Ms");

      _statusUpdateUtil.logError(_originalMessage,
                                 SchedulerAsyncCallback.class,
                                 "Task timeout",
                                 _manager.getHelixDataAccessor());
      addSummary(_resultSummaryMap, _originalMessage, _manager, true);
    }

    @Override
    public void onReplyMessage(Message message)
    {
      _logger.info("Update for scheduler msg " + _originalMessage.getMsgId()
          + " Message " + message.getMsgSrc() + " id " + message.getCorrelationId()
          + " completed");
      String key = "MessageResult " + message.getMsgSrc() + " " + UUID.randomUUID();
      _resultSummaryMap.put(key, message.getResultMap());

      if (this.isDone())
      {
        _logger.info("Scheduler msg " + _originalMessage.getMsgId() + " completed");
        _statusUpdateUtil.logInfo(_originalMessage,
                                  SchedulerAsyncCallback.class,
                                  "Scheduler task completed",
                                  _manager.getHelixDataAccessor());
        addSummary(_resultSummaryMap, _originalMessage, _manager, false);
      }
    }

    private void addSummary(Map<String, Map<String, String>> _resultSummaryMap,
                            Message originalMessage,
                            HelixManager manager,
                            boolean timeOut)
    {
      Map<String, String> summary = new TreeMap<String, String>();
      summary.put("TotalMessages:", "" + _resultSummaryMap.size());
      summary.put("Timeout", "" + timeOut);
      _resultSummaryMap.put("Summary", summary);

      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();
      String instanceName = manager.getInstanceName();
      String sessionId = manager.getSessionId();
      // ZNRecord statusUpdate
      // = accessor.getProperty(PropertyType.STATUSUPDATES_CONTROLLER,
      // MessageType.SCHEDULER_MSG.toString(), originalMessage.getMsgId());
      ZNRecord statusUpdate =
          accessor.getProperty(keyBuilder.controllerTaskStatus(instanceName,
                                                               sessionId,
                                                               MessageType.SCHEDULER_MSG.toString(),
                                                               originalMessage.getMsgId()))
                  .getRecord();

      statusUpdate.getMapFields().putAll(_resultSummaryMap);

      // accessor.setProperty(PropertyType.STATUSUPDATES_CONTROLLER,
      // statusUpdate,
      // MessageType.SCHEDULER_MSG.toString(),
      // originalMessage.getMsgId());
      accessor.setProperty(keyBuilder.controllerTaskStatus(instanceName,
                                                           sessionId,
                                                           MessageType.SCHEDULER_MSG.toString(),
                                                           originalMessage.getMsgId()),
                           new StatusUpdate(statusUpdate));

    }
  }

  private static Logger _logger =
                                    Logger.getLogger(DefaultSchedulerMessageHandlerFactory.class);
  HelixManager          _manager;

  public DefaultSchedulerMessageHandlerFactory(HelixManager manager)
  {
    _manager = manager;
  }

  @Override
  public MessageHandler createHandler(Message message, NotificationContext context)
  {
    String type = message.getMsgType();

    if (!type.equals(getMessageType()))
    {
      throw new HelixException("Unexpected msg type for message " + message.getMsgId()
          + " type:" + message.getMsgType());
    }

    return new DefaultSchedulerMessageHandler(message, context, _manager);
  }

  @Override
  public String getMessageType()
  {
    return MessageType.SCHEDULER_MSG.toString();
  }

  @Override
  public void reset()
  {
  }

  public static class DefaultSchedulerMessageHandler extends MessageHandler
  {
    HelixManager _manager;

    public DefaultSchedulerMessageHandler(Message message,
                                          NotificationContext context,
                                          HelixManager manager)
    {
      super(message, context);
      _manager = manager;
    }

    @Override
    public HelixTaskResult handleMessage() throws InterruptedException
    {
      String type = _message.getMsgType();
      HelixTaskResult result = new HelixTaskResult();
      if (!type.equals(MessageType.SCHEDULER_MSG.toString()))
      {
        throw new HelixException("Unexpected msg type for message " + _message.getMsgId()
            + " type:" + _message.getMsgType());
      }
      // Parse timeout value
      int timeOut = -1;
      if (_message.getRecord().getSimpleFields().containsKey("TIMEOUT"))
      {
        try
        {
          timeOut =
              Integer.parseInt(_message.getRecord().getSimpleFields().get("TIMEOUT"));
        }
        catch (Exception e)
        {
        }
      }

      // Parse the message template
      ZNRecord record = new ZNRecord("templateMessage");
      record.getSimpleFields()
            .putAll(_message.getRecord().getMapField("MessageTemplate"));
      Message messageTemplate = new Message(record);

      // Parse the criteria
      StringReader sr = new StringReader(_message.getRecord().getSimpleField("Criteria"));
      ObjectMapper mapper = new ObjectMapper();
      Criteria recipientCriteria;
      try
      {
        recipientCriteria = mapper.readValue(sr, Criteria.class);
      }
      catch (Exception e)
      {
        _logger.error("", e);
        result.setException(e);
        result.setSuccess(false);
        return result;
      }
      _logger.info("Scheduler sending message, criteria:" + recipientCriteria);
      // Send all messages.
      int nMsgsSent =
          _manager.getMessagingService().send(recipientCriteria,
                                              messageTemplate,
                                              new SchedulerAsyncCallback(_message,
                                                                         _manager),
                                              timeOut);

      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();
      String instanceName = _manager.getInstanceName();
      String sessionId = _manager.getSessionId();

      // Record the number of messages sent into status updates
      Map<String, String> sendSummary = new HashMap<String, String>();
      sendSummary.put("MessageCount", "" + nMsgsSent);
      // ZNRecord statusUpdate =
      // _manager.getDataAccessor().getProperty(PropertyType.STATUSUPDATES_CONTROLLER,
      // MessageType.SCHEDULER_MSG.toString(),
      // _message.getMsgId());
      ZNRecord statusUpdate =
          accessor.getProperty(keyBuilder.controllerTaskStatus(instanceName,
                                                               sessionId,
                                                               MessageType.SCHEDULER_MSG.toString(),
                                                               _message.getMsgId()))
                  .getRecord();

      statusUpdate.getMapFields().put("SentMessageCount", sendSummary);
      // _manager.getDataAccessor().setProperty(PropertyType.STATUSUPDATES_CONTROLLER,
      // statusUpdate,
      // MessageType.SCHEDULER_MSG.toString(),
      // _message.getMsgId());

      accessor.setProperty(keyBuilder.controllerTaskStatus(instanceName,
                                                           sessionId,
                                                           MessageType.SCHEDULER_MSG.toString(),
                                                           _message.getMsgId()),
                           new StatusUpdate(statusUpdate));

      result.getTaskResultMap().put("ControllerResult",
                                    "msg " + _message.getMsgId() + " from "
                                        + _message.getMsgSrc() + " processed");
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type)
    {
      _logger.error("Message handling pipeline get an exception. MsgId:"
                        + _message.getMsgId(),
                    e);
    }
  }
}
