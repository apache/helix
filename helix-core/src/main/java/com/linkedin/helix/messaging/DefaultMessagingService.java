package com.linkedin.helix.messaging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.ClusterMessagingService;
import com.linkedin.helix.Criteria;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.messaging.handling.AsyncCallbackService;
import com.linkedin.helix.messaging.handling.CMTaskExecutor;
import com.linkedin.helix.messaging.handling.MessageHandlerFactory;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageType;

public class DefaultMessagingService implements ClusterMessagingService
{
  private final ClusterManager _manager;
  private final CriteriaEvaluator _evaluator;
  private final CMTaskExecutor _taskExecutor;
  // TODO:rename to factory, this is not a service
  private final AsyncCallbackService _asyncCallbackService;
  private static Logger _logger = Logger
      .getLogger(DefaultMessagingService.class);

  public DefaultMessagingService(ClusterManager manager)
  {
    _manager = manager;
    _evaluator = new CriteriaEvaluator();
    _taskExecutor = new CMTaskExecutor();
    _asyncCallbackService = new AsyncCallbackService();
    _taskExecutor.registerMessageHandlerFactory(
        MessageType.TASK_REPLY.toString(), _asyncCallbackService);
  }

  @Override
  public int send(Criteria recipientCriteria, final Message messageTemplate)
  {
    return send(recipientCriteria, messageTemplate, null, -1);
  }

  @Override
  public int send(final Criteria recipientCriteria, final Message message,
      AsyncCallback callbackOnReply, int timeOut)
  {
    return send(recipientCriteria, message, callbackOnReply, timeOut, 0);
  }

  @Override
  public int send(final Criteria recipientCriteria, final Message message,
      AsyncCallback callbackOnReply, int timeOut, int retryCount)
  {
    Map<InstanceType, List<Message>> generateMessage = generateMessage(
        recipientCriteria, message);
    int totalMessageCount = 0;
    for (List<Message> messages : generateMessage.values())
    {
      totalMessageCount += messages.size();
    }
    String correlationId = null;
    if (callbackOnReply != null)
    {
      int totalTimeout = timeOut * (retryCount + 1);
      if (totalTimeout < 0)
      {
        totalTimeout = -1;
      }
      callbackOnReply.setTimeout(totalTimeout);
      correlationId = UUID.randomUUID().toString();
      for (List<Message> messages : generateMessage.values())
      {
        callbackOnReply.setMessagesSent(messages);
      }
      _asyncCallbackService.registerAsyncCallback(correlationId,
          callbackOnReply);
    }

    for (InstanceType receiverType : generateMessage.keySet())
    {
      List<Message> list = generateMessage.get(receiverType);
      for (Message tempMessage : list)
      {
        tempMessage.setRetryCount(retryCount);
        tempMessage.setExecutionTimeout(timeOut);
        tempMessage.setSrcInstanceType(_manager.getInstanceType());
        if (correlationId != null)
        {
          tempMessage.setCorrelationId(correlationId);
        }
        if (receiverType == InstanceType.CONTROLLER)
        {
            _manager.getDataAccessor().setProperty(PropertyType.MESSAGES_CONTROLLER,
                                                   tempMessage,
                                                   tempMessage.getId());

        }
        if (receiverType == InstanceType.PARTICIPANT)
        {
          _manager.getDataAccessor().setProperty(PropertyType.MESSAGES,
                                                 tempMessage,
                                                 tempMessage.getTgtName(),
                                                 tempMessage.getId());
        }
      }
    }

    if (callbackOnReply != null)
    {
      // start timer if timeout is set
      callbackOnReply.startTimer();
    }
    return totalMessageCount;
  }

  private Map<InstanceType, List<Message>> generateMessage(
      final Criteria recipientCriteria, final Message message)
  {
    Map<InstanceType, List<Message>> messagesToSendMap = new HashMap<InstanceType, List<Message>>();
    InstanceType instanceType = recipientCriteria.getRecipientInstanceType();

    if (instanceType == InstanceType.CONTROLLER)
    {
      List<Message> messages = generateMessagesForController(message);
      messagesToSendMap.put(InstanceType.CONTROLLER, messages);
      // _dataAccessor.setControllerProperty(PropertyType.MESSAGES,
      // newMessage.getRecord(), CreateMode.PERSISTENT);
    } else if (instanceType == InstanceType.PARTICIPANT)
    {
      List<Message> messages = new ArrayList<Message>();
      List<Map<String, String>> matchedList = _evaluator.evaluateCriteria(
           recipientCriteria, _manager);

      if (!matchedList.isEmpty())
      {
        Map<String, String> sessionIdMap = new HashMap<String, String>();
        if (recipientCriteria.isSessionSpecific())
        {
          List<LiveInstance> liveInstances = _manager.getDataAccessor().getChildValues(LiveInstance.class,
                                                                                       PropertyType.LIVEINSTANCES);

          for (LiveInstance liveInstance : liveInstances)
          {
            sessionIdMap.put(liveInstance.getInstanceName(),
                liveInstance.getSessionId());
          }
        }
        for (Map<String, String> map : matchedList)
        {
          String id = UUID.randomUUID().toString();
          Message newMessage = new Message(message.getRecord(), id);
          String srcInstanceName = _manager.getInstanceName();
          String tgtInstanceName = map.get("instanceName");
          // Don't send message to self
          if (recipientCriteria.isSelfExcluded()
              && srcInstanceName.equalsIgnoreCase(tgtInstanceName))
          {
            continue;
          }
          newMessage.setSrcName(srcInstanceName);
          newMessage.setTgtName(tgtInstanceName);
          newMessage.setStateUnitGroup(map.get("resourceGroup"));
          newMessage.setStateUnitKey(map.get("resourceKey"));
          if (recipientCriteria.isSessionSpecific())
          {
            newMessage.setTgtSessionId(sessionIdMap.get(tgtInstanceName));
          }
          messages.add(newMessage);
        }
        messagesToSendMap.put(InstanceType.PARTICIPANT, messages);
      }
    }
    return messagesToSendMap;
  }

  private List<Message> generateMessagesForController(Message message)
  {
    List<Message> messages = new ArrayList<Message>();
    String id = UUID.randomUUID().toString();
    Message newMessage = new Message(message.getRecord(), id);
    newMessage.setMsgId(id);
    newMessage.setSrcName(_manager.getInstanceName());
    newMessage.setTgtName("Controller");
    messages.add(newMessage);
    return messages;
  }

  @Override
  public void registerMessageHandlerFactory(String type,
      MessageHandlerFactory factory)
  {
    _logger.info("adding msg factory for type " + type);
    _taskExecutor.registerMessageHandlerFactory(type, factory);
    // Self-send a no-op message, so that the onMessage() call will be invoked
    // again, and
    // we have a chance to process the message that we received with the new
    // added MessageHandlerFactory
    // before the factory is added.
    sendNopMessage();
  }

  public void sendNopMessage()
  {
    if (_manager.isConnected())
    {
      try
      {
        Message nopMsg = new Message(MessageType.NO_OP, UUID.randomUUID()
            .toString());
        nopMsg.setSrcName(_manager.getInstanceName());

        if (_manager.getInstanceType() == InstanceType.CONTROLLER
            || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT)
        {
          nopMsg.setTgtName("Controller");
          _manager.getDataAccessor().setProperty(PropertyType.MESSAGES_CONTROLLER,
                                                 nopMsg,
                                                 nopMsg.getId());
        }

        if (_manager.getInstanceType() == InstanceType.PARTICIPANT
            || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT)
        {
          nopMsg.setTgtName(_manager.getInstanceName());
          _manager.getDataAccessor().setProperty(PropertyType.MESSAGES,
                                                 nopMsg,
                                                 nopMsg.getTgtName(),
                                                 nopMsg.getId());
        }

      }
      catch (Exception e)
      {
        _logger.error(e);
      }
    }
  }

  public CMTaskExecutor getExecutor()
  {
    return _taskExecutor;
  }

  @Override
  public int sendAndWait(Criteria receipientCriteria, Message message,
      AsyncCallback asyncCallback, int timeOut, int retryCount)
  {
    int messagesSent = send(receipientCriteria, message, asyncCallback, timeOut, retryCount);
    if (messagesSent > 0)
    {
      while (!asyncCallback.isDone() && !asyncCallback.isTimedOut())
      {
        synchronized (asyncCallback)
        {
          try
          {
            asyncCallback.wait();
          } catch (InterruptedException e)
          {
            _logger.error(e);
            asyncCallback.setInterrupted(true);
            break;
          }
        }
      }
    } else
    {
      _logger.warn("No messages sent. For Criteria:" + receipientCriteria);
    }
    return messagesSent;
  }

  @Override
  public int sendAndWait(Criteria recipientCriteria, Message message,
      AsyncCallback asyncCallback, int timeOut)
  {
    return sendAndWait(recipientCriteria, message, asyncCallback, timeOut, 0);
  }
  
  public static void main(String[] args) throws Exception
  {
    try
    {
      throw new Exception();
    }
    catch(Exception e)
    {
      int c = 0;
      c++;
      throw e;
    }
    finally
    {
      int x = 0;
      x++;
    }
  }
}
