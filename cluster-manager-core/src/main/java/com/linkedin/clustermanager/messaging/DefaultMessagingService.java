package com.linkedin.clustermanager.messaging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.AsyncCallback;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.ControllerPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterMessagingService;
import com.linkedin.clustermanager.Criteria;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.ExternalView;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.Message;

public class DefaultMessagingService implements ClusterMessagingService
{
  private final ClusterManager _manager;
  private CriteriaEvaluator _evaluator;
  private ClusterDataAccessor _dataAccessor;
  private Map<String, AsyncCallback> _asyncCallbackMap;

  public DefaultMessagingService(ClusterManager manager)
  {
    _evaluator = new CriteriaEvaluator();
    _manager = manager;
    _dataAccessor = _manager.getDataAccessor();
    _asyncCallbackMap = new HashMap<String, AsyncCallback>();
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
    if (callbackOnReply != null)
    {
      String correlationId = UUID.randomUUID().toString();

      for (List<Message> messages : generateMessage.values())
      {
        totalMessageCount += messages.size();
        callbackOnReply.setMessageSentCount(totalMessageCount);
        callbackOnReply.setMessagesSent(generateMessage);
      }
      _asyncCallbackMap.put(correlationId, callbackOnReply);
    }

    for (InstanceType receiverType : generateMessage.keySet())
    {
      List<Message> list = generateMessage.get(receiverType);
      for (Message tempMessage : list)
      {
        tempMessage.setId(UUID.randomUUID().toString());
        if (receiverType == InstanceType.CONTROLLER)
        {
          _dataAccessor.setControllerProperty(ControllerPropertyType.MESSAGES,
              tempMessage.getRecord(), CreateMode.PERSISTENT);
        }
        if (receiverType == InstanceType.PARTICIPANT)
        {
          _dataAccessor.setInstanceProperty(message.getTgtName(),
              InstancePropertyType.MESSAGES, tempMessage.getId(),
              tempMessage.getRecord());
        }
      }
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
      List<Map<String, String>> clusterData = prepareInputFromClusterData();

      List<Map<String, String>> matchedList = _evaluator.evaluateCriteria(
          clusterData, recipientCriteria);

      if (!matchedList.isEmpty())
      {
        Map<String, String> sessionIdMap = new HashMap<String, String>();
        if (recipientCriteria.isSessionSpecific())
        {
          List<ZNRecord> clusterPropertyList = _dataAccessor
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

  private List<Map<String, String>> prepareInputFromClusterData()
  {
    // todo:optimize and read only resource groups needed
    List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
    List<ZNRecord> recordList = _dataAccessor
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
          row.put("resourceKey", name);
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
    newMessage.setSrcName(_manager.getInstanceName());
    messages.add(newMessage);
    return messages;
  }

}
