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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.PropertyKey;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.Attributes;

public class GroupMessageHandler {
  class CurrentStateUpdate {
    final PropertyKey _key;
    final CurrentState _curStateDelta;

    public CurrentStateUpdate(PropertyKey key, CurrentState curStateDelta) {
      _key = key;
      _curStateDelta = curStateDelta;
    }

    public void merge(CurrentState curState) {
      _curStateDelta.getRecord().merge(curState.getRecord());
    }
  }

  static class GroupMessageInfo {
    final Message _message;
    final AtomicInteger _countDown;
    final ConcurrentLinkedQueue<CurrentStateUpdate> _curStateUpdateList;

    public GroupMessageInfo(Message message) {
      _message = message;
      List<PartitionId> partitionNames = message.getPartitionIds();
      _countDown = new AtomicInteger(partitionNames.size());
      _curStateUpdateList = new ConcurrentLinkedQueue<CurrentStateUpdate>();
    }

    public Map<PropertyKey, CurrentState> merge() {
      Map<String, CurrentStateUpdate> curStateUpdateMap = new HashMap<String, CurrentStateUpdate>();
      for (CurrentStateUpdate update : _curStateUpdateList) {
        String path = update._key.getPath();
        if (!curStateUpdateMap.containsKey(path)) {
          curStateUpdateMap.put(path, update);
        } else {
          curStateUpdateMap.get(path).merge(update._curStateDelta);
        }
      }

      Map<PropertyKey, CurrentState> ret = new HashMap<PropertyKey, CurrentState>();
      for (CurrentStateUpdate update : curStateUpdateMap.values()) {
        ret.put(update._key, update._curStateDelta);
      }

      return ret;
    }

  }

  final ConcurrentHashMap<String, GroupMessageInfo> _groupMsgMap;

  public GroupMessageHandler() {
    _groupMsgMap = new ConcurrentHashMap<String, GroupMessageInfo>();
  }

  public void put(Message message) {
    _groupMsgMap.putIfAbsent(message.getId(), new GroupMessageInfo(message));
  }

  // return non-null if all sub-messages are completed
  public GroupMessageInfo onCompleteSubMessage(Message subMessage) {
    String parentMid = subMessage.getAttribute(Attributes.PARENT_MSG_ID);
    GroupMessageInfo info = _groupMsgMap.get(parentMid);
    if (info != null) {
      int val = info._countDown.decrementAndGet();
      if (val <= 0) {
        return _groupMsgMap.remove(parentMid);
      }
    }

    return null;
  }

  void addCurStateUpdate(Message subMessage, PropertyKey key, CurrentState delta) {
    String parentMid = subMessage.getAttribute(Attributes.PARENT_MSG_ID);
    GroupMessageInfo info = _groupMsgMap.get(parentMid);
    if (info != null) {
      info._curStateUpdateList.add(new CurrentStateUpdate(key, delta));
    }

  }
}
