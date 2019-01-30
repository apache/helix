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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache for holding pending messages in all instances in the given cluster.
 */
public class InstanceMessagesCache {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceMessagesCache.class.getName());
  private Map<String, Map<String, Message>> _messageMap;
  private Map<String, Map<String, Message>> _relayMessageMap;

  // maintain a cache of participant messages across pipeline runs
  // <instance -> {<MessageId, Message>}>
  private Map<String, Map<String, Message>> _messageCache = Maps.newHashMap();

  // maintain a set of valid pending P2P messages.
  // <instance -> {<MessageId, Message>}>
  private Map<String, Map<String, Message>> _relayMessageCache = Maps.newHashMap();

  // Map of a relay message to its original hosted message.
  private Map<String, Message> _relayHostMessageCache = Maps.newHashMap();

  public static final String RELAY_MESSAGE_LIFETIME = "helix.controller.messagecache.relaymessagelifetime";

  // If Helix missed all of other events to evict a relay message from the cache, it will delete the message anyway after this timeout.
  private static final int DEFAULT_RELAY_MESSAGE_LIFETIME = 120 * 1000;  // in ms
  private final int _relayMessageLifetime;

  private String _clusterName;

  public InstanceMessagesCache(String clusterName) {
    _clusterName = clusterName;
    _relayMessageLifetime = HelixUtil
        .getSystemPropertyAsInt(RELAY_MESSAGE_LIFETIME, DEFAULT_RELAY_MESSAGE_LIFETIME);
  }

  /**
   * This refreshes all pending messages in the cluster by re-fetching the data from zookeeper in an
   * efficient way current state must be refreshed before refreshing relay messages because we need
   * to use current state to validate all relay messages.
   *
   * @param accessor
   * @param liveInstanceMap
   *
   * @return
   */
  public boolean refresh(HelixDataAccessor accessor, Map<String, LiveInstance> liveInstanceMap) {
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
      List<Message> newMessages = accessor.getProperty(newMessageKeys, true);
      for (Message message : newMessages) {
        if (message != null) {
          Map<String, Message> cachedMap = _messageCache.get(message.getTgtName());
          cachedMap.put(message.getId(), message);
        }
      }
    }

    _messageMap = Collections.unmodifiableMap(msgMap);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Message purge took: {} ", purgeSum);

    }

    LOG.info(
        "END: InstanceMessagesCache.refresh(), {} of Messages read from ZooKeeper. took {} ms. ",
        newMessageKeys.size(), (System.currentTimeMillis() - startTime));
    return true;
  }

  /**
   * Refresh relay message cache by updating relay messages read from ZK, and remove all expired relay messages.
   */
  public void updateRelayMessages(Map<String, LiveInstance> liveInstanceMap,
      Map<String, Map<String, Map<String, CurrentState>>> currentStateMap) {

    // cache all relay messages read from ZK
    for (String instance : _messageMap.keySet()) {
      Map<String, Message> instanceMessages = _messageMap.get(instance);
      for (Message message : instanceMessages.values()) {
        if (message.hasRelayMessages()) {
          for (Message relayMsg : message.getRelayMessages().values()) {
            cacheRelayMessage(relayMsg, message);
          }
        }
      }
    }

    Map<String, Map<String, Message>> relayMessageMap = new HashMap<>();
    long nextRebalanceTime = Long.MAX_VALUE;

    // Iterate all relay message in the cache, remove invalid or expired ones.
    for (String instance : _relayMessageCache.keySet()) {
      Map<String, Message> relayMessages = _relayMessageCache.get(instance);
      Iterator<Map.Entry<String, Message>> iterator = relayMessages.entrySet().iterator();

      while (iterator.hasNext()) {
        Message relayMessage = iterator.next().getValue();

        Map<String, Message> instanceMsgMap = _messageMap.get(instance);
        if (instanceMsgMap != null && instanceMsgMap.containsKey(relayMessage.getMsgId())) {
          Message committedMessage = instanceMsgMap.get(relayMessage.getMsgId());
          if (committedMessage.isRelayMessage()) {
            LOG.info("Relay message committed, remove relay message {} from the cache.", relayMessage
                .getId());
            iterator.remove();
            _relayHostMessageCache.remove(relayMessage.getMsgId());
            continue;
          } else {
            // controller already sent the same message to target host,
            // To avoid potential race-condition, do not remove relay message immediately,
            // just set the relay time as current time .
            if (relayMessage.getRelayTime() < 0) {
              relayMessage.setRelayTime(System.currentTimeMillis());
              LOG.info(
                  "Controller already sent the message {} to the target host, set message to be relayed at {}",
                  relayMessage.getId(), relayMessage.getRelayTime());
            }
          }
        }

        String sessionId = relayMessage.getTgtSessionId();
        String instanceSessionId = liveInstanceMap.get(instance).getSessionId();

        // Target host's session has been changed, remove relay message
        if (!instanceSessionId.equals(sessionId)) {
          LOG.info("Instance SessionId does not match, remove relay message {} from the cache.", relayMessage.getId());
          iterator.remove();
          _relayHostMessageCache.remove(relayMessage.getMsgId());
          continue;
        }

        Map<String, Map<String, CurrentState>> instanceCurrentStateMap =
            currentStateMap.get(instance);
        if (instanceCurrentStateMap == null) {
          LOG.warn("CurrentStateMap null for " + instance);
          continue;
        }
        Map<String, CurrentState> sessionCurrentStateMap = instanceCurrentStateMap.get(sessionId);
        if (sessionCurrentStateMap == null) {
          LOG.warn("CurrentStateMap null for {}, session {}.", instance, sessionId);
          continue;
        }

        String resourceName = relayMessage.getResourceName();
        String partitionName = relayMessage.getPartitionName();
        String targetState = relayMessage.getToState();
        String fromState = relayMessage.getFromState();
        CurrentState currentState = sessionCurrentStateMap.get(resourceName);

        long currentTime = System.currentTimeMillis();
        if (currentState == null) {
          if (relayMessage.getRelayTime() < 0) {
            relayMessage.setRelayTime(currentTime);
            LOG.warn("CurrentState is null for {} on {}, set relay time {} for message {}",
                resourceName, instance, relayMessage.getRelayTime(), relayMessage.getId());
          }
        }

        // if partition state on the target host already changed, set it to be expired.
        String partitionState = currentState.getState(partitionName);
        if (targetState.equals(partitionState) || !fromState.equals(partitionState)) {
          if (relayMessage.getRelayTime() < 0) {
            relayMessage.setRelayTime(currentTime);
            LOG.debug(
                "CurrentState {} on target host has changed, set relay time {} for message {}.",
                partitionState, relayMessage.getRelayTime(), relayMessage.getId());
          }
        }

        // derive relay time from hosted message and relayed host.
        setRelayTime(relayMessage, liveInstanceMap, currentStateMap);

        if (relayMessage.isExpired()) {
          LOG.info("relay message {} expired, remove it from cache. relay time {}.",
              relayMessage.getId(), relayMessage.getRelayTime());
          iterator.remove();
          _relayHostMessageCache.remove(relayMessage.getMsgId());
          continue;
        }

        // If Helix missed all of other events to evict a relay message from the cache,
        // it will delete the message anyway after a certain timeout.
        // This is the latest resort to avoid a relay message stuck in the cache forever.
        // This case should happen very rarely.
        if (relayMessage.getRelayTime() < 0
            && (relayMessage.getCreateTimeStamp() + _relayMessageLifetime) < System
            .currentTimeMillis()) {
          LOG.info(
              "relay message {} has reached its lifetime, remove it from cache.", relayMessage.getId());
          iterator.remove();
          _relayHostMessageCache.remove(relayMessage.getMsgId());
          continue;
        }

        long expiryTime = relayMessage.getCreateTimeStamp() + _relayMessageLifetime;
        if (relayMessage.getRelayTime() > 0) {
          expiryTime = relayMessage.getRelayTime() + relayMessage.getExpiryPeriod();
        }

        if (expiryTime < nextRebalanceTime) {
          nextRebalanceTime = expiryTime;
        }

        if (!relayMessageMap.containsKey(instance)) {
          relayMessageMap.put(instance, Maps.<String, Message>newHashMap());
        }
        relayMessageMap.get(instance).put(relayMessage.getMsgId(), relayMessage);
      }
    }

    if (nextRebalanceTime < Long.MAX_VALUE) {
      scheduleFuturePipeline(nextRebalanceTime);
    }

    _relayMessageMap = Collections.unmodifiableMap(relayMessageMap);

    long relayCount = 0;
    // Add valid relay message to the instance message map.
    for (String instance : _relayMessageMap.keySet()) {
      Map<String, Message> relayMessages = _relayMessageMap.get(instance);
      if (!_messageMap.containsKey(instance)) {
        _messageMap.put(instance, Maps.<String, Message>newHashMap());
      }
      _messageMap.get(instance).putAll(relayMessages);
      relayCount += relayMessages.size();
    }

    if (LOG.isDebugEnabled()) {
      if (relayCount > 0) {
        LOG.debug("Relay message cache size " + relayCount);
      }
    }
  }

  // Schedule a future rebalance pipeline run.
  private void scheduleFuturePipeline(long rebalanceTime) {
    GenericHelixController controller = GenericHelixController.getController(_clusterName);
    if (controller != null) {
      controller.scheduleRebalance(rebalanceTime);
    } else {
      LOG.warn(
          "Failed to schedule a future pipeline run for cluster {} at delay {}, helix controller is null.",
          _clusterName, (rebalanceTime - System.currentTimeMillis()));
    }
  }

  private void setRelayTime(Message relayMessage, Map<String, LiveInstance> liveInstanceMap,
      Map<String, Map<String, Map<String, CurrentState>>> currentStateMap) {

    // relay time already set, avoid to reset it to a later time.
    if (relayMessage.getRelayTime() > relayMessage.getCreateTimeStamp()) {
      return;
    }

    Message hostedMessage = _relayHostMessageCache.get(relayMessage.getMsgId());
    String sessionId = hostedMessage.getTgtSessionId();
    String instance = hostedMessage.getTgtName();
    String resourceName = hostedMessage.getResourceName();
    String instanceSessionId = liveInstanceMap.get(instance).getSessionId();

    long currentTime = System.currentTimeMillis();
    long expiredTime = currentTime + relayMessage.getExpiryPeriod();

    if (!instanceSessionId.equals(sessionId)) {
      LOG.debug(
          "Hosted Instance SessionId {} does not match sessionId {} in hosted message , set relay message {} to be expired at {}, hosted message ",
          instanceSessionId, sessionId, relayMessage.getId(), expiredTime,
          hostedMessage.getMsgId());
      relayMessage.setRelayTime(currentTime);
      return;
    }

    Map<String, Map<String, CurrentState>> instanceCurrentStateMap = currentStateMap.get(instance);
    if (instanceCurrentStateMap == null) {
      LOG.debug(
          "No instanceCurrentStateMap found for {} on {}, set relay messages {} to be expired at {}"
              + resourceName, instance, relayMessage.getId(), expiredTime);
      relayMessage.setRelayTime(currentTime);
      return;
    }

    Map<String, CurrentState> sessionCurrentStateMap = instanceCurrentStateMap.get(sessionId);
    if (sessionCurrentStateMap == null) {
      LOG.debug("No sessionCurrentStateMap found, set relay messages {} to be expired at {}. ",
          relayMessage.getId(), expiredTime);
      relayMessage.setRelayTime(currentTime);
      return;
    }

    String partitionName = hostedMessage.getPartitionName();
    String targetState = hostedMessage.getToState();
    String fromState = hostedMessage.getFromState();

    CurrentState currentState = sessionCurrentStateMap.get(resourceName);
    if (currentState == null) {
      LOG.debug("No currentState found for {} on {}, set relay message {} to be expired at {} ",
          resourceName, instance, relayMessage.getId(),
          (currentTime + relayMessage.getExpiryPeriod()));
      relayMessage.setRelayTime(currentTime);
      return;
    }

    if (targetState.equals(currentState.getState(partitionName))) {
      long completeTime = currentState.getEndTime(partitionName);
      if (completeTime < relayMessage.getCreateTimeStamp()) {
        completeTime = currentTime;
      }
      relayMessage.setRelayTime(completeTime);
      LOG.debug(
          "Target state match the hosted message's target state, set relay message {} relay time at {}.",
          relayMessage.getId(), completeTime);
    }

    if (!fromState.equals(currentState.getState(partitionName))) {
      LOG.debug(
          "Current state does not match hosted message's from state, set relay message {} relay time at {}.",
          relayMessage.getId(), currentTime);
      relayMessage.setRelayTime(currentTime);
    }
  }

  /**
   * Provides a list of current outstanding pending state transition messages on a given instance.
   *
   * @param instanceName
   *
   * @return
   */
  public Map<String, Message> getMessages(String instanceName) {
    if (_messageMap.containsKey(instanceName)) {
      return _messageMap.get(instanceName);
    }
    return Collections.emptyMap();
  }

  /**
   * Provides a list of current outstanding pending relay (p2p) messages on a given instance.
   *
   * @param instanceName
   *
   * @return
   */
  public Map<String, Message> getRelayMessages(String instanceName) {
    if (_relayMessageMap.containsKey(instanceName)) {
      return _relayMessageMap.get(instanceName);
    }
    return Collections.emptyMap();
  }

  public void cacheMessages(Collection<Message> messages) {
    for (Message message : messages) {
      String instanceName = message.getTgtName();
      if (!_messageCache.containsKey(instanceName)) {
        _messageCache.put(instanceName, Maps.<String, Message>newHashMap());
      }
      _messageCache.get(instanceName).put(message.getId(), message);

      if (message.hasRelayMessages()) {
        for (Message relayMsg : message.getRelayMessages().values()) {
          cacheRelayMessage(relayMsg, message);
        }
      }
    }
  }

  private void cacheRelayMessage(Message relayMessage, Message hostMessage) {
    String instanceName = relayMessage.getTgtName();
    if (!_relayMessageCache.containsKey(instanceName)) {
      _relayMessageCache.put(instanceName, Maps.<String, Message>newHashMap());
    }
    _relayMessageCache.get(instanceName).put(relayMessage.getId(), relayMessage);
    _relayHostMessageCache.put(relayMessage.getMsgId(), hostMessage);

    LOG.info("Add relay message to relay cache " + relayMessage.getMsgId() + ", hosted message "
        + hostMessage.getMsgId());
  }

  @Override public String toString() {
    return "InstanceMessagesCache{" +
        "_messageMap=" + _messageMap +
        ", _messageCache=" + _messageCache +
        ", _clusterName='" + _clusterName + '\'' +
        '}';
  }
}
