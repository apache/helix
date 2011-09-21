package com.linkedin.clustermanager.controller;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.Message;

public class PendingMessageStateHolder
{
  private LinkedHashMap<String, Map<String, String>> _pendingMessageStateMap;

  PendingMessageStateHolder()
  {
    _pendingMessageStateMap = new LinkedHashMap<String, Map<String, String>>();
  }

  public String getState(String dbPartition, String instanceName)
  {
    Map<String, String> map = _pendingMessageStateMap.get(instanceName);
    if (map != null)
    {
      return map.get(dbPartition);
    }
    return null;
  }

  public void refresh(String instanceName, String currentSessionId,
      List<ZNRecord> messages)
  {
    Map<String, String> map = new HashMap<String, String>();
    for (ZNRecord record : messages)
    {
      Message message = new Message(record);
      if (message.getTgtSessionId().equals(currentSessionId))
      {
        map.put(message.getStateUnitKey(), message.getToState());
      }
    }
    _pendingMessageStateMap.put(instanceName, map);
  }

}
