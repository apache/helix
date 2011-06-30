package com.linkedin.clustermanager.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.Message;

/**
 * Holds the state to which the instance might go to if the task[i.e message] is
 * succesfully processed by the storage/relay node This is needed for the
 * calculation of best possible ideal state.
 * 
 * @author kgopalak
 */
public class MessageHolder
{

  private Map<String, Map<String, String>> _messageStateMap;

  public MessageHolder()
  {
    _messageStateMap = new HashMap<String, Map<String, String>>();
  }

  public void update(String instanceName, List<ZNRecord> messages)
  {
    Map<String, String> map = new HashMap<String, String>();
    for (ZNRecord record : messages)
    {
      Message message;
      if (record instanceof Message)
      {
        message = (Message) record;
      } else
      {
        // TODO remove this after upgrading jackson to handle derived
        // class
        message = new Message(record);
      }

      String stateUnitKey = message.getStateUnitKey();
      String value = message.getToState();
      map.put(stateUnitKey, value);
    }
    _messageStateMap.put(instanceName, map);
  }

  public String get(String stateUnitKey, String instanceName)
  {
    Map<String, String> map = _messageStateMap.get(instanceName);
    if (map != null)
    {
      return map.get(stateUnitKey);
    }
    return null;
  }

}
