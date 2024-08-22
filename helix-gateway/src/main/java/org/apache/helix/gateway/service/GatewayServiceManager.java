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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.ImmutableSet;
import org.apache.helix.gateway.channel.GatewayServiceChannelConfig;
import org.apache.helix.gateway.channel.HelixGatewayServiceChannelFactory;
import org.apache.helix.gateway.api.constant.GatewayServiceEventType;
import org.apache.helix.gateway.api.service.HelixGatewayServiceChannel;
import org.apache.helix.gateway.participant.HelixGatewayParticipant;
import org.apache.helix.gateway.util.PerKeyBlockingExecutor;


/**
 * A top layer class that send/receive messages from Grpc end point, and dispatch them to corrsponding gateway services.
 *  1. get event from Grpc service
 *  2. Maintain a gateway service registry, one gateway service maps to one Helix cluster
 *  3. On init connect, create the participant manager
 *  4. For ST reply message, update the tracker
 */
public class GatewayServiceManager {
  public static final int CONNECTION_EVENT_THREAD_POOL_SIZE = 10;
  public static final ImmutableSet<String> SUPPORTED_MULTI_STATE_MODEL_TYPES =
      ImmutableSet.of("OnlineOffline");
  private final Map<String, Map<String, HelixGatewayParticipant>> _helixGatewayParticipantMap;
  private final String _zkAddress;

  // a single thread tp for event processing
  private final ExecutorService _participantStateTransitionResultUpdator;

  // link to grpc service
  private final HelixGatewayServiceChannel _gatewayServiceChannel;

  // a per key executor for connection event. All event for the same instance will be executed in sequence.
  // It is used to ensure for each instance, the connect/disconnect event won't start until the previous one is done.
  private final PerKeyBlockingExecutor _connectionEventProcessor;

  private final GatewayServiceChannelConfig _gatewayServiceChannelConfig;

  public GatewayServiceManager(String zkAddress, GatewayServiceChannelConfig gatewayServiceChannelConfig) {
    _helixGatewayParticipantMap = new ConcurrentHashMap<>();
    _zkAddress = zkAddress;
    _participantStateTransitionResultUpdator = Executors.newSingleThreadExecutor();
    _gatewayServiceChannel = HelixGatewayServiceChannelFactory.createServiceChannel(gatewayServiceChannelConfig, this);
    _connectionEventProcessor =
        new PerKeyBlockingExecutor(CONNECTION_EVENT_THREAD_POOL_SIZE); // todo: make it configurable
    _gatewayServiceChannelConfig = gatewayServiceChannelConfig;
  }

  /**
   * Process the event from Grpc service and dispatch to async executor for processing.
   *
   * @param event
   */
  public void onGatewayServiceEvent(GatewayServiceEvent event) {
    if (event.getEventType().equals(GatewayServiceEventType.UPDATE)) {
      _participantStateTransitionResultUpdator.submit(new shardStateUpdator(event));
    } else {
      _connectionEventProcessor.offerEvent(event.getInstanceName(), new ParticipantConnectionProcessor(event));
    }
  }

  /**
   * Update in memory shard state
   */
  class shardStateUpdator implements Runnable {

    private final GatewayServiceEvent _event;

    private shardStateUpdator(GatewayServiceEvent event) {
      _event = event;
    }

    @Override
    public void run() {
      HelixGatewayParticipant participant =
          getHelixGatewayParticipant(_event.getClusterName(), _event.getInstanceName());
      if (participant == null) {
        // TODO: return error code and throw exception.
        return;
      }
      _event.getStateTransitionResult().forEach(stateTransitionResult -> {
        participant.completeStateTransition(stateTransitionResult.getStateTransitionId(),
            stateTransitionResult.getIsSuccess());
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


  public void startService() throws IOException {
    _gatewayServiceChannel.start();
  }

  public void stopService() {
    _gatewayServiceChannel.stop();
    _connectionEventProcessor.shutdown();
    _participantStateTransitionResultUpdator.shutdown();
    _helixGatewayParticipantMap.clear();
  }

  private void createHelixGatewayParticipant(String clusterName, String instanceName,
      Map<String, Map<String, String>> initialShardStateMap) {
    // Create and add the participant to the participant map
    HelixGatewayParticipant.Builder participantBuilder =
        new HelixGatewayParticipant.Builder(_gatewayServiceChannel, instanceName, clusterName,
            _zkAddress,
            () -> removeHelixGatewayParticipant(clusterName, instanceName)).setInitialShardState(
            initialShardStateMap);
    SUPPORTED_MULTI_STATE_MODEL_TYPES.forEach(
        participantBuilder::addMultiTopStateStateModelDefinition);
    _helixGatewayParticipantMap.computeIfAbsent(clusterName, k -> new ConcurrentHashMap<>())
        .put(instanceName, participantBuilder.build());
  }

  private void removeHelixGatewayParticipant(String clusterName, String instanceName) {
    // Disconnect and remove the participant from the participant map
    HelixGatewayParticipant participant = getHelixGatewayParticipant(clusterName, instanceName);
    if (participant != null) {
      participant.disconnect();
      _helixGatewayParticipantMap.get(clusterName).remove(instanceName);
    }
  }

  private HelixGatewayParticipant getHelixGatewayParticipant(String clusterName,
      String instanceName) {
    return _helixGatewayParticipantMap.getOrDefault(clusterName, Collections.emptyMap())
        .get(instanceName);
  }
}
