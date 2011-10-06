package com.linkedin.clustermanager.messaging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.ControllerPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterMessagingService;
import com.linkedin.clustermanager.Criteria;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.messaging.handling.AsyncCallbackService;
import com.linkedin.clustermanager.messaging.handling.CMTaskExecutor;
import com.linkedin.clustermanager.messaging.handling.MessageHandlerFactory;
import com.linkedin.clustermanager.model.ExternalView;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;

public class DefaultMessagingService implements ClusterMessagingService
{
  private static Logger logger = Logger
      .getLogger(DefaultMessagingService.class);

  private final ClusterManager _manager;
  private CriteriaEvaluator _evaluator;
  private final CMTaskExecutor _taskExecutor;
  // TODO:rename to factory, this is not a service
  private final AsyncCallbackService _asyncCallbackService;
  private static Logger _logger = Logger
      .getLogger(DefaultMessagingService.class);

  public DefaultMessagingService(ClusterManager manager)
  {
    _evaluator = new CriteriaEvaluator();
    _manager = manager;
    _taskExecutor = new CMTaskExecutor();
    _asyncCallbackService = new AsyncCallbackService();
    registerMessageHandlerFactory(MessageType.TASK_REPLY.toString(),
        _asyncCallbackService);

  }

  @Override
  public int send(Criteria recipientCriteria, final Message messageTemplate)
  {
    return send(recipientCriteria, messageTemplate, null);
  }

  @Override
  public int send(final Criteria recipientCriteria, final Message message,
      AsyncCallback callbackOnReply)
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
        if (correlationId != null)
        {
          tempMessage.setCorrelationId(correlationId);
        }
        if (receiverType == InstanceType.CONTROLLER)
        {
          _manager.getDataAccessor().setControllerProperty(
              ControllerPropertyType.MESSAGES, tempMessage.getRecord(),
              CreateMode.PERSISTENT);
        }
        if (receiverType == InstanceType.PARTICIPANT)
        {
          _manager.getDataAccessor().setInstanceProperty(
              tempMessage.getTgtName(), InstancePropertyType.MESSAGES,
              tempMessage.getId(), tempMessage.getRecord());
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
      // _dataAccessor.setControllerProperty(ControllerPropertyType.MESSAGES,
      // newMessage.getRecord(), CreateMode.PERSISTENT);
    } else if (instanceType == InstanceType.PARTICIPANT)
    {
      List<Message> messages = new ArrayList<Message>();
      List<Map<String, String>> clusterData = prepareInputFromClusterData(recipientCriteria);

      List<Map<String, String>> matchedList = _evaluator.evaluateCriteria(
          clusterData, recipientCriteria);

      if (!matchedList.isEmpty())
      {
        Map<String, String> sessionIdMap = new HashMap<String, String>();
        if (recipientCriteria.isSessionSpecific())
        {
          List<ZNRecord> clusterPropertyList = _manager.getDataAccessor()
              .getClusterPropertyList(ClusterPropertyType.LIVEINSTANCES);
          for (ZNRecord znRecord : clusterPropertyList)
          {
            LiveInstance liveInstance = new LiveInstance(znRecord);
            sessionIdMap.put(liveInstance.getInstanceName(),
                liveInstance.getSessionId());
          }
        }
        for (Map<String, String> map : matchedList)
        {
          Message newMessage = new Message(message.getRecord());
          newMessage.setSrcName(_manager.getInstanceName());
          newMessage.setTgtName(map.get("instanceName"));
          newMessage.setStateUnitGroup(map.get("resourceGroup"));
          newMessage.setStateUnitKey(map.get("resourceKey"));
          newMessage.setMsgId(UUID.randomUUID().toString());
          newMessage.setId(newMessage.getMsgId());
          if (recipientCriteria.isSessionSpecific())
          {
            newMessage.setTgtName(sessionIdMap.get(map.get("instanceName")));
          }
          messages.add(newMessage);
        }
        messagesToSendMap.put(InstanceType.PARTICIPANT, messages);
      }
    }
    return messagesToSendMap;
  }

  private List<Map<String, String>> prepareInputFromClusterData(
      Criteria criteria)
  {
    // todo:optimize and read only resource groups needed
    List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
    List<ZNRecord> recordList = _manager.getDataAccessor()
        .getClusterPropertyList(ClusterPropertyType.EXTERNALVIEW);
    for (ZNRecord record : recordList)
    {
      ExternalView view = new ExternalView(record);
      Set<String> resourceKeys = view.getResourceKeys();
      for (String resourceKeyName : resourceKeys)
      {
        Map<String, String> stateMap = view.getStateMap(resourceKeyName);
        for (String name : stateMap.keySet())
        {
          Map<String, String> row = new HashMap<String, String>();
          row.put("instanceName", name);
          row.put("resourceGroup", view.getResourceGroup());
          row.put("state", stateMap.get(name));
          row.put("resourceKey", resourceKeyName);
          rows.add(row);
        }
      }
    }
    return rows;
  }

  private List<Message> generateMessagesForController(Message message)
  {
    List<Message> messages = new ArrayList<Message>();
    Message newMessage = new Message(message.getRecord());
    newMessage.setId(message.getId());
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
  }

  public CMTaskExecutor getExecutor()
  {
    return _taskExecutor;
  }

  @Override
  public int sendAndWait(Criteria receipientCriteria, Message message,
      AsyncCallback asyncCallback)
  {
    int messagesSent = send(receipientCriteria, message, asyncCallback);
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
      logger.warn("No messages sent. For Criteria:" + receipientCriteria);
    }
    return messagesSent;
  }
}
