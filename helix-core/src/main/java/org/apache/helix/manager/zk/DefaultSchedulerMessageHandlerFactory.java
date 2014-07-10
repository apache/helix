package org.apache.helix.manager.zk;

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

import java.io.StringReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StatusUpdate;
import org.apache.helix.util.StatusUpdateUtil;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

/*
 * The current implementation supports throttling on STATE-TRANSITION type of message, transition SCHEDULED-COMPLETED.
 *
 */
public class DefaultSchedulerMessageHandlerFactory implements MessageHandlerFactory {
  public static final String WAIT_ALL = "WAIT_ALL";
  public static final String SCHEDULER_MSG_ID = "SchedulerMessageId";
  public static final String SCHEDULER_TASK_QUEUE = "SchedulerTaskQueue";
  public static final String CONTROLLER_MSG_ID = "controllerMsgId";
  public static final int TASKQUEUE_BUCKET_NUM = 10;

  public static class SchedulerAsyncCallback extends AsyncCallback {
    StatusUpdateUtil _statusUpdateUtil = new StatusUpdateUtil();
    Message _originalMessage;
    HelixManager _manager;
    final Map<String, Map<String, String>> _resultSummaryMap =
        new ConcurrentHashMap<String, Map<String, String>>();

    public SchedulerAsyncCallback(Message originalMessage, HelixManager manager) {
      _originalMessage = originalMessage;
      _manager = manager;
    }

    @Override
    public void onTimeOut() {
      _logger.info("Scheduler msg timeout " + _originalMessage.getMessageId() + " timout with "
          + _timeout + " Ms");

      _statusUpdateUtil.logError(_originalMessage, SchedulerAsyncCallback.class, "Task timeout",
          _manager.getHelixDataAccessor());
      addSummary(_resultSummaryMap, _originalMessage, _manager, true);
    }

    @Override
    public void onReplyMessage(Message message) {
      _logger.info("Update for scheduler msg " + _originalMessage.getMessageId() + " Message "
          + message.getMsgSrc() + " id " + message.getCorrelationId() + " completed");
      String key = "MessageResult " + message.getMsgSrc() + " " + UUID.randomUUID();
      _resultSummaryMap.put(key, message.getResultMap());

      if (this.isDone()) {
        _logger.info("Scheduler msg " + _originalMessage.getMessageId() + " completed");
        _statusUpdateUtil.logInfo(_originalMessage, SchedulerAsyncCallback.class,
            "Scheduler task completed", _manager.getHelixDataAccessor());
        addSummary(_resultSummaryMap, _originalMessage, _manager, false);
      }
    }

    private void addSummary(Map<String, Map<String, String>> _resultSummaryMap,
        Message originalMessage, HelixManager manager, boolean timeOut) {
      Map<String, String> summary = new TreeMap<String, String>();
      summary.put("TotalMessages:", "" + _resultSummaryMap.size());
      summary.put("Timeout", "" + timeOut);
      _resultSummaryMap.put("Summary", summary);

      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();
      ZNRecord statusUpdate =
          accessor.getProperty(
              keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), originalMessage
                  .getMessageId().stringify())).getRecord();

      statusUpdate.getMapFields().putAll(_resultSummaryMap);
      accessor.setProperty(keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(),
          originalMessage.getMessageId().stringify()), new StatusUpdate(statusUpdate));

    }
  }

  private static Logger _logger = Logger.getLogger(DefaultSchedulerMessageHandlerFactory.class);
  HelixManager _manager;

  public DefaultSchedulerMessageHandlerFactory(HelixManager manager) {
    _manager = manager;
  }

  @Override
  public MessageHandler createHandler(Message message, NotificationContext context) {
    String type = message.getMsgType();

    if (!type.equals(getMessageType())) {
      throw new HelixException("Unexpected msg type for message " + message.getMessageId()
          + " type:" + message.getMsgType());
    }

    return new DefaultSchedulerMessageHandler(message, context, _manager);
  }

  @Override
  public String getMessageType() {
    return MessageType.SCHEDULER_MSG.toString();
  }

  @Override
  public void reset() {
  }

  public static class DefaultSchedulerMessageHandler extends MessageHandler {
    HelixManager _manager;

    public DefaultSchedulerMessageHandler(Message message, NotificationContext context,
        HelixManager manager) {
      super(message, context);
      _manager = manager;
    }

    void handleMessageUsingScheduledTaskQueue(Criteria recipientCriteria, Message messageTemplate,
        MessageId controllerMsgId) {
      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();

      Map<String, String> sendSummary = new HashMap<String, String>();
      sendSummary.put("MessageCount", "0");
      Map<InstanceType, List<Message>> messages =
          _manager.getMessagingService().generateMessage(recipientCriteria, messageTemplate);

      // Calculate tasks, and put them into the idealState of the SCHEDULER_TASK_QUEUE resource.
      // List field are the destination node, while the Message parameters are stored in the
      // mapFields
      // task throttling can be done on SCHEDULER_TASK_QUEUE resource
      if (messages.size() > 0) {
        String taskQueueName = _message.getRecord().getSimpleField(SCHEDULER_TASK_QUEUE);
        if (taskQueueName == null) {
          throw new HelixException("SchedulerTaskMessage need to have " + SCHEDULER_TASK_QUEUE
              + " specified.");
        }
        IdealState newAddedScheduledTasks = new IdealState(taskQueueName);
        newAddedScheduledTasks.setBucketSize(TASKQUEUE_BUCKET_NUM);
        newAddedScheduledTasks.setStateModelDefId(StateModelDefId.from(SCHEDULER_TASK_QUEUE));

        synchronized (_manager) {
          int existingTopPartitionId = 0;
          IdealState currentTaskQueue =
              _manager.getHelixDataAccessor().getProperty(
                  accessor.keyBuilder().idealStates(newAddedScheduledTasks.getId()));
          if (currentTaskQueue != null) {
            existingTopPartitionId = findTopPartitionId(currentTaskQueue) + 1;
          }

          List<Message> taskMessages = (List<Message>) (messages.values().toArray()[0]);
          for (Message task : taskMessages) {
            String partitionId = taskQueueName + "_" + existingTopPartitionId;
            existingTopPartitionId++;
            String instanceName = task.getTgtName();
            newAddedScheduledTasks.setPartitionState(PartitionId.from(partitionId),
                ParticipantId.from(instanceName), State.from("COMPLETED"));
            task.getRecord().setSimpleField(instanceName, "COMPLETED");
            task.getRecord().setSimpleField(CONTROLLER_MSG_ID, controllerMsgId.stringify());

            List<String> priorityList = new LinkedList<String>();
            priorityList.add(instanceName);
            newAddedScheduledTasks.getRecord().setListField(partitionId, priorityList);
            newAddedScheduledTasks.getRecord().setMapField(partitionId,
                task.getRecord().getSimpleFields());
            _logger.info("Scheduling for controllerMsg " + controllerMsgId + " , sending task "
                + partitionId + " " + task.getMessageId() + " to " + instanceName);

            if (_logger.isDebugEnabled()) {
              _logger.debug(task.getRecord().getSimpleFields());
            }
          }
          _manager.getHelixDataAccessor().updateProperty(
              accessor.keyBuilder().idealStates(newAddedScheduledTasks.getId()),
              newAddedScheduledTasks);
          sendSummary.put("MessageCount", "" + taskMessages.size());
        }
      }
      // Record the number of messages sent into scheduler message status updates

      ZNRecord statusUpdate =
          accessor.getProperty(
              keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), _message
                  .getMessageId().stringify())).getRecord();

      statusUpdate.getMapFields().put("SentMessageCount", sendSummary);
      accessor.updateProperty(keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(),
          _message.getMessageId().stringify()), new StatusUpdate(statusUpdate));
    }

    private int findTopPartitionId(IdealState currentTaskQueue) {
      int topId = 0;
      for (PartitionId partitionId : currentTaskQueue.getPartitionIdSet()) {
        try {
          String partitionName = partitionId.stringify();
          String partitionNumStr = partitionName.substring(partitionName.lastIndexOf('_') + 1);
          int num = Integer.parseInt(partitionNumStr);
          if (topId < num) {
            topId = num;
          }
        } catch (Exception e) {
          _logger.error("", e);
        }
      }
      return topId;
    }

    @Override
    public HelixTaskResult handleMessage() throws InterruptedException {
      String type = _message.getMsgType();
      HelixTaskResult result = new HelixTaskResult();
      if (!type.equals(MessageType.SCHEDULER_MSG.toString())) {
        throw new HelixException("Unexpected msg type for message " + _message.getMessageId()
            + " type:" + _message.getMsgType());
      }
      // Parse timeout value
      int timeOut = -1;
      if (_message.getRecord().getSimpleFields().containsKey("TIMEOUT")) {
        try {
          timeOut = Integer.parseInt(_message.getRecord().getSimpleFields().get("TIMEOUT"));
        } catch (Exception e) {
        }
      }

      // Parse the message template
      ZNRecord record = new ZNRecord("templateMessage");
      record.getSimpleFields().putAll(_message.getRecord().getMapField("MessageTemplate"));
      Message messageTemplate = new Message(record);

      // Parse the criteria
      StringReader sr = new StringReader(_message.getRecord().getSimpleField("Criteria"));
      ObjectMapper mapper = new ObjectMapper();
      Criteria recipientCriteria;
      try {
        recipientCriteria = mapper.readValue(sr, Criteria.class);
      } catch (Exception e) {
        _logger.error("", e);
        result.setException(e);
        result.setSuccess(false);
        return result;
      }
      _logger.info("Scheduler sending message, criteria:" + recipientCriteria);

      boolean waitAll = false;
      if (_message.getRecord().getSimpleField(DefaultSchedulerMessageHandlerFactory.WAIT_ALL) != null) {
        try {
          waitAll =
              Boolean.parseBoolean(_message.getRecord().getSimpleField(
                  DefaultSchedulerMessageHandlerFactory.WAIT_ALL));
        } catch (Exception e) {
          _logger.warn("", e);
        }
      }
      boolean hasSchedulerTaskQueue =
          _message.getRecord().getSimpleFields().containsKey(SCHEDULER_TASK_QUEUE);
      // If the target is PARTICIPANT, use the ScheduledTaskQueue
      if (InstanceType.PARTICIPANT == recipientCriteria.getRecipientInstanceType()
          && hasSchedulerTaskQueue) {
        handleMessageUsingScheduledTaskQueue(recipientCriteria, messageTemplate,
            _message.getMessageId());
        result.setSuccess(true);
        result.getTaskResultMap().put(SCHEDULER_MSG_ID, _message.getMessageId().stringify());
        result.getTaskResultMap().put("ControllerResult",
            "msg " + _message.getMessageId() + " from " + _message.getMsgSrc() + " processed");
        return result;
      }

      _logger.info("Scheduler sending message to Controller");
      int nMsgsSent = 0;
      SchedulerAsyncCallback callback = new SchedulerAsyncCallback(_message, _manager);
      if (waitAll) {
        nMsgsSent =
            _manager.getMessagingService().sendAndWait(recipientCriteria, messageTemplate,
                callback, timeOut);
      } else {
        nMsgsSent =
            _manager.getMessagingService().send(recipientCriteria, messageTemplate, callback,
                timeOut);
      }

      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();

      // Record the number of messages sent into status updates
      Map<String, String> sendSummary = new HashMap<String, String>();
      sendSummary.put("MessageCount", "" + nMsgsSent);

      ZNRecord statusUpdate =
          accessor.getProperty(
              keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), _message
                  .getMessageId().stringify())).getRecord();

      statusUpdate.getMapFields().put("SentMessageCount", sendSummary);

      accessor.setProperty(keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(),
          _message.getMessageId().stringify()), new StatusUpdate(statusUpdate));

      result.getTaskResultMap().put("ControllerResult",
          "msg " + _message.getMessageId() + " from " + _message.getMsgSrc() + " processed");
      result.getTaskResultMap().put(SCHEDULER_MSG_ID, _message.getMessageId().stringify());
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
