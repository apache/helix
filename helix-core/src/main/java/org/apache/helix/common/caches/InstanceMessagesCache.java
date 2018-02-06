package org.apache.helix.common.caches;

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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache for holding pending messages in all instances in the given cluster.
 */
public class InstanceMessagesCache {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceMessagesCache.class.getName());
  private Map<String, Map<String, Message>> _messageMap;

  // maintain a cache of participant messages across pipeline runs
  private Map<String, Map<String, Message>> _messageCache = Maps.newHashMap();
  private String _clusterName;

  public InstanceMessagesCache(String clusterName) {
    _clusterName = clusterName;
  }

  /**
   * This refreshes all pending messages in the cluster by re-fetching the data from zookeeper in an
   * efficient way
   * current state must be refreshed before refreshing relay messages because we need to use current
   * state to validate all relay messages.
   *
   * @param accessor
   * @param liveInstanceMap
   *
   * @return
   */
  public synchronized boolean refresh(HelixDataAccessor accessor,
      Map<String, LiveInstance> liveInstanceMap) {
    LOG.info("START: InstanceMessagesCache.refresh()");
    long startTime = System.currentTimeMillis();

    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Map<String, Map<String, Message>> msgMap = new HashMap<>();
    List<PropertyKey> newMessageKeys = Lists.newLinkedList();
    long purgeSum = 0;
    for (String instanceName : liveInstanceMap.keySet()) {
      // get the cache
      Map<String, Message> cachedMap = _messageCache.get(instanceName);
      if (cachedMap == null) {
        cachedMap = Maps.newHashMap();
        _messageCache.put(instanceName, cachedMap);
      }
      msgMap.put(instanceName, cachedMap);

      // get the current names
      Set<String> messageNames =
          Sets.newHashSet(accessor.getChildNames(keyBuilder.messages(instanceName)));

      long purgeStart = System.currentTimeMillis();
      // clear stale names
      Iterator<String> cachedNamesIter = cachedMap.keySet().iterator();
      while (cachedNamesIter.hasNext()) {
        String messageName = cachedNamesIter.next();
        if (!messageNames.contains(messageName)) {
          cachedNamesIter.remove();
        }
      }
      long purgeEnd = System.currentTimeMillis();
      purgeSum += purgeEnd - purgeStart;

      // get the keys for the new messages
      for (String messageName : messageNames) {
        if (!cachedMap.containsKey(messageName)) {
          newMessageKeys.add(keyBuilder.message(instanceName, messageName));
        }
      }
    }

    // get the new messages
    if (newMessageKeys.size() > 0) {
      List<Message> newMessages = accessor.getProperty(newMessageKeys);
      for (Message message : newMessages) {
        if (message != null) {
          Map<String, Message> cachedMap = _messageCache.get(message.getTgtName());
          cachedMap.put(message.getId(), message);
        }
      }
    }

    _messageMap = Collections.unmodifiableMap(msgMap);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Message purge took: " + purgeSum);
      LOG.debug("# of Messages read from ZooKeeper " + newMessageKeys.size() + ". took " + (
          System.currentTimeMillis() - startTime) + " ms.");
    }

    return true;
  }

  // update all valid relay messages attached to existing state transition messages into message map.
  public void updateRelayMessages(Map<String, LiveInstance> liveInstanceMap,
      Map<String, Map<String, Map<String, CurrentState>>> currentStateMap) {
    List<Message> relayMessages = new ArrayList<>();
    for (String instance : _messageMap.keySet()) {
      Map<String, Message> instanceMessages = _messageMap.get(instance);
      Map<String, Map<String, CurrentState>> instanceCurrentStateMap =
          currentStateMap.get(instance);
      if (instanceCurrentStateMap == null) {
        continue;
      }

      for (Message message : instanceMessages.values()) {
        if (message.hasRelayMessages()) {
          String sessionId = message.getTgtSessionId();
          String resourceName = message.getResourceName();
          String partitionName = message.getPartitionName();
          String targetState = message.getToState();
          String instanceSessionId = liveInstanceMap.get(instance).getSessionId();

          if (!instanceSessionId.equals(sessionId)) {
            continue;
          }

          Map<String, CurrentState> sessionCurrentStateMap = instanceCurrentStateMap.get(sessionId);
          if (sessionCurrentStateMap == null) {
            continue;
          }
          CurrentState currentState = sessionCurrentStateMap.get(resourceName);
          if (currentState == null || !targetState.equals(currentState.getState(partitionName))) {
            continue;
          }
          long transitionCompleteTime = currentState.getEndTime(partitionName);

          for (Message msg : message.getRelayMessages().values()) {
            msg.setRelayTime(transitionCompleteTime);
            if (!message.isExpired()) {
              relayMessages.add(msg);
            }
          }
        }
      }
    }

    for (Message message : relayMessages) {
      String instance = message.getTgtName();
      Map<String, Message> instanceMessages = _messageMap.get(instance);
      if (instanceMessages == null) {
        instanceMessages = new HashMap<>();
        _messageMap.put(instance, instanceMessages);
      }
      instanceMessages.put(message.getId(), message);
    }
  }

  /**
   * Provides a list of current outstanding transitions on a given instance.
   *
   * @param instanceName
   *
   * @return
   */
  public Map<String, Message> getMessages(String instanceName) {
    Map<String, Message> map = _messageMap.get(instanceName);
    if (map != null) {
      return map;
    } else {
      return Collections.emptyMap();
    }
  }

  public void cacheMessages(List<Message> messages) {
    for (Message message : messages) {
      String instanceName = message.getTgtName();
      Map<String, Message> instMsgMap;
      if (_messageCache.containsKey(instanceName)) {
        instMsgMap = _messageCache.get(instanceName);
      } else {
        instMsgMap = Maps.newHashMap();
        _messageCache.put(instanceName, instMsgMap);
      }
      instMsgMap.put(message.getId(), message);
    }
  }

  @Override public String toString() {
    return "InstanceMessagesCache{" +
        "_messageMap=" + _messageMap +
        ", _messageCache=" + _messageCache +
        ", _clusterName='" + _clusterName + '\'' +
        '}';
  }
}
