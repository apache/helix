package org.apache.helix.gateway.service;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.helix.gateway.api.constant.GatewayServiceEventType;
import org.apache.helix.gateway.api.service.HelixGatewayServiceChannel;
import org.apache.helix.gateway.channel.GatewayServiceChannelConfig;
import org.apache.helix.gateway.channel.HelixGatewayServiceChannelFactory;
import org.apache.helix.gateway.participant.HelixGatewayParticipant;
import org.apache.helix.gateway.util.GatewayCurrentStateCache;
import org.apache.helix.gateway.util.PerKeyBlockingExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.gateway.api.constant.GatewayServiceManagerConstant.*;


/**
 * A top layer class that send/receive messages from Grpc end point, and dispatch them to corrsponding gateway services.
 *  1. get event from Grpc service
 *  2. Maintain a gateway service registry, one gateway service maps to one Helix cluster
 *  3. On init connect, create the participant manager
 *  4. For ST reply message, update the tracker
 */
public class GatewayServiceManager {
  private static final Logger logger = LoggerFactory.getLogger(GatewayServiceManager.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  public static final int CONNECTION_EVENT_THREAD_POOL_SIZE = 10;
  public static final ImmutableSet<String> SUPPORTED_MULTI_STATE_MODEL_TYPES =
      ImmutableSet.of("OnlineOffline");
  private final Map<String, Map<String, HelixGatewayParticipant>> _helixGatewayParticipantMap;
  private final String _zkAddress;

  // a single thread tp for event processing
  private final ExecutorService _participantStateTransitionResultUpdator;

  // link to grpc service
  private HelixGatewayServiceChannel _gatewayServiceChannel;

  // a per key executor for connection event. All event for the same instance will be executed in sequence.
  // It is used to ensure for each instance, the connect/disconnect event won't start until the previous one is done.
  private final PerKeyBlockingExecutor _connectionEventProcessor;

  private final Map<String, GatewayCurrentStateCache> _currentStateCacheMap;

  public GatewayServiceManager(String zkAddress) {
    _helixGatewayParticipantMap = new ConcurrentHashMap<>();
    _zkAddress = zkAddress;
    _participantStateTransitionResultUpdator = Executors.newSingleThreadExecutor();
    _connectionEventProcessor =
        new PerKeyBlockingExecutor(CONNECTION_EVENT_THREAD_POOL_SIZE); // todo: make it configurable
    _currentStateCacheMap = new HashMap<>();
  }

  public GatewayServiceManager(String zkAddress, GatewayServiceChannelConfig gatewayServiceChannelConfig) {
    this(zkAddress);
    _gatewayServiceChannel = HelixGatewayServiceChannelFactory.createServiceChannel(gatewayServiceChannelConfig, this);
  }

  /**
   * Set the gateway service channel. This can only be called once.
   * The channel is used to send state transition message to the participant.
   *
   * @param channel the gateway service channel
   * @throws IllegalStateException if the channel is already set
   */
  public void setGatewayServiceChannel(HelixGatewayServiceChannel channel) {
    if (_gatewayServiceChannel != null) {
      throw new IllegalStateException(
          "Gateway service channel is already set, it can only be set once.");
    }
    _gatewayServiceChannel = channel;
  }

  /**
   * Process the event from Grpc service and dispatch to async executor for processing.
   *
   * @param event
   */
  public void onGatewayServiceEvent(GatewayServiceEvent event) {
    if (event.getEventType().equals(GatewayServiceEventType.UPDATE)) {
      _participantStateTransitionResultUpdator.submit(new ShardStateUpdator(event));
    } else {
      _connectionEventProcessor.offerEvent(event.getInstanceName(), new ParticipantConnectionProcessor(event));
    }
  }

  public void resetTargetStateCache(String clusterName, String instanceName) {
    logger.info("Resetting target state cache for cluster: {}, instance: {}", clusterName, instanceName);
    getOrCreateCache(clusterName).resetTargetStateCache(instanceName);
  }

  /**
   * Overwrite the current state cache with the new current state map, and return the diff of the change.
   * @param clusterName
   * @param newCurrentStateMap
   * @return
   */
  public  Map<String, Map<String, Map<String, String>>> updateCacheWithNewCurrentStateAndGetDiff(String clusterName,
      Map<String, Map<String, Map<String, String>>> newCurrentStateMap) {
   return getOrCreateCache(clusterName).updateCacheWithNewCurrentStateAndGetDiff(newCurrentStateMap);
  }

  public void updateCurrentState(String clusterName, String instanceName, String resourceId, String shardId, String toState) {
    getOrCreateCache(clusterName).updateCurrentStateOfExistingInstance(instanceName, resourceId, shardId, toState);
  }

  public synchronized String serializeTargetState() {
    ObjectNode targetStateNode = new ObjectMapper().createObjectNode();
    ObjectNode res = new ObjectMapper().createObjectNode();
    for (String clusterName : _currentStateCacheMap.keySet()) {
      // add the json node to the target state node
      targetStateNode.set(clusterName, getOrCreateCache(clusterName).serializeTargetAssignmentsToJSONNode());
    }
    res.set(TARGET_STATE_ASSIGNMENT_KEY_NAME, targetStateNode);
    res.set(TIMESTAMP_KEY, objectMapper.valueToTree(System.currentTimeMillis()));
    return res.toString();
  }

  public void updateTargetState(String clusterName, String instanceName, String resourceId, String shardId,
      String toState) {
    getOrCreateCache(clusterName).updateTargetStateOfExistingInstance(instanceName, resourceId, shardId, toState);
  }

  public String getCurrentState(String clusterName, String instanceName, String resourceId, String shardId) {
    return getOrCreateCache(clusterName).getCurrentState(instanceName, resourceId, shardId);
  }

  public String getTargetState(String clusterName, String instanceName, String resourceId, String shardId) {
    return getOrCreateCache(clusterName).getTargetState(instanceName, resourceId, shardId);
  }

  public Map<String, Map<String, Map<String, String>>> getAllTargetStates(String clusterName) {
    return getOrCreateCache(clusterName).getAllTargetStates();
  }

  /**
   * Update in memory shard state
   */
  class ShardStateUpdator implements Runnable {

    private final GatewayServiceEvent _event;

    private ShardStateUpdator(GatewayServiceEvent event) {
      _event = event;
    }

    @Override
    public void run() {
      logger.info("Processing state transition result " + _event.getInstanceName());
      HelixGatewayParticipant participant =
          getHelixGatewayParticipant(_event.getClusterName(), _event.getInstanceName());
      if (participant == null) {
        // TODO: return error code and throw exception.
        return;
      }
      _event.getStateTransitionResult().forEach(stateTransitionResult -> {
        participant.completeStateTransition(stateTransitionResult.getResourceName(),
            stateTransitionResult.getShardName(), stateTransitionResult.getShardState());
      });
    }
  }

  /**
   * Create HelixGatewayService instance and register it to the manager.
   * It includes waiting for ZK connection, and also wait for previous LiveInstance to expire.
   */
  class ParticipantConnectionProcessor implements Runnable {
    GatewayServiceEvent _event;

    public ParticipantConnectionProcessor(GatewayServiceEvent event) {
      _event = event;
    }

    @Override
    public void run() {
      if (_event.getEventType().equals(GatewayServiceEventType.CONNECT)) {
        createHelixGatewayParticipant(_event.getClusterName(), _event.getInstanceName(),
            _event.getShardStateMap());
      } else {
        removeHelixGatewayParticipant(_event.getClusterName(), _event.getInstanceName());
      }
    }
  }

  public void stopManager() {
    _connectionEventProcessor.shutdown();
    _participantStateTransitionResultUpdator.shutdown();
    _helixGatewayParticipantMap.forEach((clusterName, participantMap) -> {
      participantMap.forEach((instanceName, participant) -> {
        participant.disconnect();
      });
    });
  }

  public void startService() throws IOException {
    _gatewayServiceChannel.start();
  }

  public void stopService() {
    _gatewayServiceChannel.stop();
    stopManager();
  }

  private void createHelixGatewayParticipant(String clusterName, String instanceName,
      Map<String, Map<String, String>> initialShardStateMap) {
    resetTargetStateCache(clusterName, instanceName);
    // Create and add the participant to the participant map
    HelixGatewayParticipant.Builder participantBuilder =
        new HelixGatewayParticipant.Builder(_gatewayServiceChannel, instanceName, clusterName, _zkAddress,
            () -> removeHelixGatewayParticipant(clusterName, instanceName), this).setInitialShardState(
            initialShardStateMap);
    SUPPORTED_MULTI_STATE_MODEL_TYPES.forEach(participantBuilder::addMultiTopStateStateModelDefinition);
    _helixGatewayParticipantMap.computeIfAbsent(clusterName, k -> new ConcurrentHashMap<>())
        .put(instanceName, participantBuilder.build());
  }

  private void removeHelixGatewayParticipant(String clusterName, String instanceName) {
    logger.info("Removing participant: {} from cluster: {}", instanceName, clusterName);
    // Disconnect and remove the participant from the participant map
    HelixGatewayParticipant participant = getHelixGatewayParticipant(clusterName, instanceName);
    if (participant != null) {
      participant.disconnect();
      if (_helixGatewayParticipantMap.containsKey(clusterName)) {
        _helixGatewayParticipantMap.get(clusterName).remove(instanceName);
      }
    }
    if (_currentStateCacheMap.containsKey(clusterName)) {
      _currentStateCacheMap.get(clusterName).removeInstanceTargetDataFromCache(instanceName);
    }
  }

  private HelixGatewayParticipant getHelixGatewayParticipant(String clusterName,
      String instanceName) {
    return _helixGatewayParticipantMap.getOrDefault(clusterName, Collections.emptyMap())
        .get(instanceName);
  }

  private synchronized GatewayCurrentStateCache getOrCreateCache(String clusterName) {
    return _currentStateCacheMap.computeIfAbsent(clusterName, k -> new GatewayCurrentStateCache(clusterName));
  }
}
