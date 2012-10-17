package com.linkedin.helix.messaging.handling;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.Attributes;

public class GroupMessageHandler
{
  class CurrentStateUpdate
  {
    final PropertyKey  _key;
    final CurrentState _curStateDelta;

    public CurrentStateUpdate(PropertyKey key, CurrentState curStateDelta)
    {
      _key = key;
      _curStateDelta = curStateDelta;
    }

    public void merge(CurrentState curState)
    {
      _curStateDelta.getRecord().merge(curState.getRecord());
    }
  }

  static class GroupMessageInfo
  {
    final Message                                   _message;
    final AtomicInteger                             _countDown;
    final ConcurrentLinkedQueue<CurrentStateUpdate> _curStateUpdateList;

    public GroupMessageInfo(Message message)
    {
      _message = message;
      List<String> partitionNames = message.getPartitionNames();
      _countDown = new AtomicInteger(partitionNames.size());
      _curStateUpdateList = new ConcurrentLinkedQueue<CurrentStateUpdate>();
    }
    
    public Map<PropertyKey, CurrentState> merge()
    {
      Map<String, CurrentStateUpdate> curStateUpdateMap =
          new HashMap<String, CurrentStateUpdate>();
      for (CurrentStateUpdate update : _curStateUpdateList)
      {
        String path = update._key.getPath();
        if (!curStateUpdateMap.containsKey(path))
        {
          curStateUpdateMap.put(path, update);
        }
        else
        {
          curStateUpdateMap.get(path).merge(update._curStateDelta);
        }
      }

      Map<PropertyKey, CurrentState> ret = new HashMap<PropertyKey, CurrentState>();
      for (CurrentStateUpdate update : curStateUpdateMap.values())
      {
        ret.put(update._key, update._curStateDelta);
      }

      return ret;
    }
 
  }

  final ConcurrentHashMap<String, GroupMessageInfo> _groupMsgMap;

  public GroupMessageHandler()
  {
    _groupMsgMap = new ConcurrentHashMap<String, GroupMessageInfo>();
  }

  public void put(Message message)
  {
    _groupMsgMap.putIfAbsent(message.getId(), new GroupMessageInfo(message));
  }

  // return non-null if all sub-messages are completed
  public GroupMessageInfo onCompleteSubMessage(Message subMessage)
  {
    String parentMid = subMessage.getAttribute(Attributes.PARENT_MSG_ID);
    GroupMessageInfo info = _groupMsgMap.get(parentMid);
    if (info != null)
    {
      int val = info._countDown.decrementAndGet();
      if (val <= 0)
      {
        return _groupMsgMap.remove(parentMid);
      }
    }

    return null;
  }

  void addCurStateUpdate(Message subMessage, PropertyKey key, CurrentState delta)
  {
    String parentMid = subMessage.getAttribute(Attributes.PARENT_MSG_ID);
    GroupMessageInfo info = _groupMsgMap.get(parentMid);
    if (info != null)
    {
      info._curStateUpdateList.add(new CurrentStateUpdate(key, delta));
    }

  }
}
