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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.RebalanceUtil;
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
  private static final long DEFAULT_RELAY_MESSAGE_LIFETIME = TimeUnit.MINUTES.toMillis(60);  // in ms
  private final long _relayMessageLifetime;

  private String _clusterName;

  public InstanceMessagesCache(String clusterName) {
    _clusterName = clusterName;
    _relayMessageLifetime = HelixUtil
        .getSystemPropertyAsLong(RELAY_MESSAGE_LIFETIME, DEFAULT_RELAY_MESSAGE_LIFETIME);
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

    long nextRebalanceTime = Long.MAX_VALUE;
    long currentTime = System.currentTimeMillis();
    Map<String, Map<String, Message>> relayMessageMap = new HashMap<>();
    Set<String> targetInstanceToRemove = new HashSet<>();
    // Iterate all relay message in the cache, remove invalid or expired ones.
    for (String targetInstance : _relayMessageCache.keySet()) {
      Map<String, Message> relayMessages = _relayMessageCache.get(targetInstance);
      Iterator<Map.Entry<String, Message>> iterator = relayMessages.entrySet().iterator();

      while (iterator.hasNext()) {
        Message relayMessage = iterator.next().getValue();
        Map<String, Message> instanceMsgMap = _messageMap.get(targetInstance);

        if (!relayMessage.isValid()) {
          LOG.warn("Invalid relay message {}, remove it from the cache.", relayMessage.getId());
          iterator.remove();
          _relayHostMessageCache.remove(relayMessage.getMsgId());
          continue;
        }

        // Check whether the relay message has already been sent to the target host.
        if (instanceMsgMap != null && instanceMsgMap.containsKey(relayMessage.getMsgId())) {
          Message committedMessage = instanceMsgMap.get(relayMessage.getMsgId());
          if (committedMessage.isRelayMessage()) {
            LOG.info("Relay message already committed, remove relay message {} from the cache.",
                relayMessage.getId());
            iterator.remove();
            _relayHostMessageCache.remove(relayMessage.getMsgId());
            continue;
          } else {
            // controller already sent the same message to target host,
            // Relay host may still forward the p2p message later, so we can not remove the relay message immediately now,
            // just set the relay time as current time.
            // TODO: we should remove the message immediately here once we have transaction id support in CurrentState.
            LOG.info(
                "Controller already sent the message to the target host, set relay message {} to be expired.",
                relayMessage.getId());
            setMessageRelayTime(relayMessage, currentTime);
          }
        }

        try {
          // Check partition's state on the relay message's target host (The relay message's destination host).
          // Set the relay message to be expired immediately or to be expired a certain time in future if necessary.
          checkTargetHost(targetInstance, relayMessage, liveInstanceMap, currentStateMap);

          // Check partition's state on the original relay host (host that should forward the relay message)
          // Set the relay message to be expired immediately or to be expired a certain time in future if necessary.
          Message hostedMessage = _relayHostMessageCache.get(relayMessage.getMsgId());
          checkRelayHost(relayMessage, liveInstanceMap, currentStateMap, hostedMessage);
        } catch (Exception e) {
          LOG.warn(
              "Failed to check target and relay host and set the relay time. Relay message: {} exception: {}",
              relayMessage.getId(), e);
        }

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

        if (!relayMessageMap.containsKey(targetInstance)) {
          relayMessageMap.put(targetInstance, Maps.<String, Message>newHashMap());
        }
        relayMessageMap.get(targetInstance).put(relayMessage.getMsgId(), relayMessage);

        // Compute the next earliest time to trigger a pipeline run.
        long expiryTime = relayMessage.getCreateTimeStamp() + _relayMessageLifetime;
        if (relayMessage.getRelayTime() > 0) {
          expiryTime = relayMessage.getRelayTime() + relayMessage.getExpiryPeriod();
        }
        if (expiryTime < nextRebalanceTime) {
          nextRebalanceTime = expiryTime;
        }
      } // end while (iterator.hasNext())

      if (relayMessages.isEmpty()) {
        targetInstanceToRemove.add(targetInstance);
      }
    }
    _relayMessageCache.keySet().removeAll(targetInstanceToRemove);

    if (nextRebalanceTime < Long.MAX_VALUE) {
      scheduleFuturePipeline(nextRebalanceTime);
    }

    _relayMessageMap = Collections.unmodifiableMap(relayMessageMap);
    long relayMessageCount = 0;

    // Add valid relay messages to the instance message map.
    for (String instance : _relayMessageMap.keySet()) {
      Map<String, Message> relayMessages = _relayMessageMap.get(instance);
      if (!_messageMap.containsKey(instance)) {
        _messageMap.put(instance, Maps.<String, Message>newHashMap());
      }
      _messageMap.get(instance).putAll(relayMessages);
      relayMessageCount += relayMessages.size();
    }

    LOG.info(
        "END: updateRelayMessages(), {} of valid relay messages in cache, took {} ms. ",
        relayMessageCount, (System.currentTimeMillis() - currentTime));
  }

  // Check partition's state on the relay message's target host (The relay message's destination host).
  // Set the relay message to be expired immediately or to be expired a certain time in future if necessary.
  private void checkTargetHost(String targetHost, Message relayMessage, Map<String, LiveInstance> liveInstanceMap,
      Map<String, Map<String, Map<String, CurrentState>>> currentStateMap) {

    long currentTime = System.currentTimeMillis();
    String resourceName = relayMessage.getResourceName();
    String partitionName = relayMessage.getPartitionName();
    String sessionId = relayMessage.getTgtSessionId();

    if (!liveInstanceMap.containsKey(targetHost)) {
      LOG.info("Target host is not alive anymore, expiring relay message {} immediately.",
          relayMessage.getId());
      relayMessage.setExpired(true);
      return;
    }

    String instanceSessionId = liveInstanceMap.get(targetHost).getEphemeralOwner();

    // Target host's session has been changed, remove relay message
    if (!instanceSessionId.equals(sessionId)) {
      LOG.info("Instance SessionId does not match, expiring relay message {} immediately.",
          relayMessage.getId());
      relayMessage.setExpired(true);
      return;
    }

    Map<String, Map<String, CurrentState>> instanceCurrentStateMap =
        currentStateMap.get(targetHost);
    if (instanceCurrentStateMap == null || !instanceCurrentStateMap.containsKey(sessionId)) {
      // This should happen only when a new session is being established.
      // We should not do anything here, once new session is established in participant side,
      // the relay message will be deleted from cache in controller's next pipeline.
      LOG.warn("CurrentStateMap null for {}, session {}, pending relay message {}", targetHost,
          sessionId, relayMessage.getId());
      return;
    }

    Map<String, CurrentState> sessionCurrentStateMap = instanceCurrentStateMap.get(sessionId);
    CurrentState currentState = sessionCurrentStateMap.get(resourceName);
    // TODO: we should add transaction id for each state transition, we can immediately delete the relay message once we have transaction id record in each currentState.
    if (currentState == null) {
      setMessageRelayTime(relayMessage, currentTime);
      LOG.warn("CurrentState is null for {} on {}, set relay time {} for message {}", resourceName,
          targetHost, relayMessage.getRelayTime(), relayMessage.getId());
      return;
    }

    // if the target partition already completed the state transition,
    // or the current state on the target partition has been changed,
    // Do not remove the message immediately to avoid race-condition,
    // for example, controller may decide to move master to another instance at this time,
    // if it does not aware of a pending relay message, it may end up with two masters.
    // so we only set the relay message to be expired.
    // TODO: we should add transaction id for each state transition, we can immediately delete the relay message once we have transaction id record in each currentState.
    String partitionCurrentState = currentState.getState(partitionName);
    String targetState = relayMessage.getToState();
    String fromState = relayMessage.getFromState();
    if (targetState.equals(partitionCurrentState) || !fromState.equals(partitionCurrentState)) {
      setMessageRelayTime(relayMessage, currentTime);
      LOG.debug("{}'s currentState {} on {} has changed, set relay message {} to be expired.",
          partitionName, partitionCurrentState, targetHost, relayMessage.getId());
    }
  }

  // Check partition's state on the original relay host (host that forwards the relay message)
  // Set the relay message to be expired immediately or to be expired a certain time in future if necessary.
  private void checkRelayHost(Message relayMessage, Map<String, LiveInstance> liveInstanceMap,
      Map<String, Map<String, Map<String, CurrentState>>> currentStateMap, Message hostedMessage) {

    long currentTime = System.currentTimeMillis();

    String sessionId = hostedMessage.getTgtSessionId();
    String relayInstance = hostedMessage.getTgtName();
    String resourceName = hostedMessage.getResourceName();
    String partitionName = hostedMessage.getPartitionName();

    if (!liveInstanceMap.containsKey(relayInstance)) {
      // If the p2p forwarding host is no longer live, we should not remove the relay message immediately
      // since we do not know whether the relay message was forwarded before the instance went offline.
      setMessageRelayTime(relayMessage, currentTime);
      return;
    }
    String instanceSessionId = liveInstanceMap.get(relayInstance).getEphemeralOwner();
    if (!instanceSessionId.equals(sessionId)) {
      LOG.info("Relay instance sessionId {} does not match sessionId {} in hosted message {}, "
              + "set relay message {} to be expired.", instanceSessionId, sessionId,
          relayMessage.getId(), hostedMessage.getMsgId());
      setMessageRelayTime(relayMessage, currentTime);
      return;
    }

    Map<String, Map<String, CurrentState>> instanceCurrentStateMap =
        currentStateMap.get(relayInstance);
    if (instanceCurrentStateMap == null || !instanceCurrentStateMap.containsKey(sessionId)) {
      LOG.warn(
          "CurrentStateMap null for {}, session {}, set relay messages {} to be expired. Hosted message {}.",
          relayInstance, sessionId, relayMessage.getId(), hostedMessage.getId());
      setMessageRelayTime(relayMessage, currentTime);
      return;
    }

    Map<String, CurrentState> sessionCurrentStateMap = instanceCurrentStateMap.get(sessionId);
    CurrentState currentState = sessionCurrentStateMap.get(resourceName);

    if (currentState == null) {
      LOG.info("No currentState found for {} on {}, set relay message {} to be expired.",
          resourceName, relayInstance, relayMessage.getId());
      setMessageRelayTime(relayMessage, currentTime);
      return;
    }

    String partitionState = currentState.getState(partitionName);
    String targetState = hostedMessage.getToState();
    String fromState = hostedMessage.getFromState();

    // The relay host partition state has been changed after relay message was created.
    if (!fromState.equals(partitionState)) {
      // If the partition on the relay host turned to ERROR while transited from top state,
      // we can remove the cached relay message right away since participant won't forward the relay message anyway.
      if (HelixDefinedState.ERROR.name().equals(partitionState) && fromState
          .equals(currentState.getPreviousState(partitionName))) {
        LOG.info("Partition {} got to ERROR from the top state, "
                + "expiring relay message {} immediately. Hosted message {}.", partitionName,
            relayMessage.getId(), hostedMessage.getId());
        relayMessage.setExpired(true);
        return;
      }

      // If the partition completed the transition, set the relay time to be the actual time when state transition completed.
      if (targetState.equals(partitionState) && fromState
          .equals(currentState.getPreviousState(partitionName))) {
        // The relay host already completed the state transition.
        long completeTime = currentState.getEndTime(partitionName);
        if (completeTime > relayMessage.getCreateTimeStamp()) {
          setMessageRelayTime(relayMessage, completeTime);
          LOG.error("Target state for partition {} matches the hosted message's target state, "
              + "set relay message {} to be expired.", partitionName, relayMessage.getId());
          return;
        }
      }

      // For all other situations, set relay time to be current time.
      setMessageRelayTime(relayMessage, currentTime);
      // the state has been changed after it completed the required state transition (maybe another state-transition happened).
      LOG.info("Current state {} for partition {} does not match hosted message's from state, "
              + "set relay message {} to be expired.", partitionState, partitionName,
          relayMessage.getId());
    }
  }

  private void setMessageRelayTime(Message relayMessage, long relayTime) {
    long currentRelayTime = relayMessage.getRelayTime();
    // relay time already set, avoid to reset it to a later time.
    if (currentRelayTime > relayMessage.getCreateTimeStamp() && currentRelayTime < relayTime) {
      return;
    }
    relayMessage.setRelayTime(relayTime);
    LOG.info("Set relay message {} relay time at {}, to be expired at {}", relayMessage.getId(),
        relayTime, (relayTime + relayMessage.getExpiryPeriod()));
  }


  // Schedule a future rebalance pipeline run.
  private void scheduleFuturePipeline(long rebalanceTime) {
      long current = System.currentTimeMillis();
      long delay = rebalanceTime - current;
      RebalanceUtil.scheduleOnDemandPipeline(_clusterName, delay);
  }

  /**
   * Provides a list of current outstanding pending state transition messages on a given instance.
   * @param instanceName
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
      _relayMessageCache.put(instanceName, Maps.<String, Message> newHashMap());
    }
    if (!_relayMessageCache.get(instanceName).containsKey(relayMessage.getId())) {
      // Only log if the message doesn't already exist in the cache
      LOG.info("Add relay message to relay cache " + relayMessage.getMsgId() + ", hosted message "
          + hostMessage.getMsgId());
    }
    _relayMessageCache.get(instanceName).put(relayMessage.getId(), relayMessage);
    _relayHostMessageCache.put(relayMessage.getMsgId(), hostMessage);
  }

  @Override public String toString() {
    return "InstanceMessagesCache{" +
        "_messageMap=" + _messageMap +
        ", _messageCache=" + _messageCache +
        ", _clusterName='" + _clusterName + '\'' +
        '}';
  }
}
