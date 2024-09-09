package org.apache.helix.gateway.channel;

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
import org.apache.commons.lang3.NotImplementedException;
import org.apache.helix.gateway.api.service.HelixGatewayServiceChannel;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.helix.gateway.service.GatewayServiceManager;
import org.apache.helix.gateway.util.StateTransitionMessageTranslateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.gateway.channel.GatewayServiceChannelConfig.FileBasedConfigType.*;
import static org.apache.helix.gateway.util.PollChannelUtil.*;


/**
 * Helix Gateway Service Poll mode implementation.
 * It periodically polls the current state of the participants and the liveness of the participants.
 */
public class HelixGatewayServicePollModeChannel implements HelixGatewayServiceChannel {
  private static final Logger logger = LoggerFactory.getLogger(HelixGatewayServicePollModeChannel.class);
  final GatewayServiceManager _manager;
  final GatewayServiceChannelConfig _config;

  // cluster -> file for user to report shards' current states
  final String _userCurrentStateFilePath;
  // cluster -> file path to store the shards' target states
  final String _targetStateFilePath;
  final GatewayServiceChannelConfig.ChannelType _participantConnectionStatusChannelType;
  final GatewayServiceChannelConfig.ChannelType _shardStateChannelType;

  // cluster -> host -> liveness result
  final Map<String, Map<String, Boolean>> _livenessResults;
  // cluster -> host -> endpoint for query liveness
  // It is the file pass if _participantConnectionStatusChannelType is FILE, grpc endpoint if it is GRPC_CLIENT
  final Map<String, Map<String, String>> _livenessCheckEndpointMap;

  ScheduledExecutorService _scheduler;

  public HelixGatewayServicePollModeChannel(GatewayServiceManager manager, GatewayServiceChannelConfig config) {
    _manager = manager;
    _config = config;
    _scheduler = Executors.newSingleThreadScheduledExecutor();
    _participantConnectionStatusChannelType = _config.getParticipantConnectionChannelType();
    _shardStateChannelType = _config.getShardStateChannelType();
    _livenessCheckEndpointMap = _config.getParticipantLivenessEndpointMap();
    _userCurrentStateFilePath = _config.getPollModeConfig(PARTICIPANT_CURRENT_STATE_PATH);
    _targetStateFilePath = _config.getPollModeConfig(SHARD_TARGET_STATE_PATH);
    _livenessResults = new HashMap<>();
  }

  /**
   * Fetch the updates from the participants.
   * 1. Get the diff of previous and current shard states, and send the state change event to the gateway manager.
   * 2. Compare previous liveness and current liveness, and send the connection event to the gateway manager.
   */
 protected  void fetchUpdates() {
    // 1.  get the shard state change
    Map<String, Map<String, Map<String, Map<String, String>>>> currentShardStates =
        getChangedParticipantsCurrentState(_userCurrentStateFilePath);

    Map<String, Map<String, Map<String, Map<String, String>>>> currentStateDiff = new HashMap<>();
    for (String clusterName : currentShardStates.keySet()) {
      Map<String, Map<String, Map<String, String>>> clusterDiffMap =
          _manager.updateCacheWithNewCurrentStateAndGetDiff(clusterName, currentShardStates.get(clusterName));
      if (clusterDiffMap == null || clusterDiffMap.isEmpty()) {
        continue;
      }
      for (String instanceName : clusterDiffMap.keySet()) {
        // if the instance is previously connected, send state change event
        if (_livenessResults.get(clusterName) != null && _livenessResults.get(clusterName).get(instanceName)) {
          logger.info("Host {} has state change, sending event to gateway manager", instanceName);
          pushClientEventToGatewayManager(_manager,
              StateTransitionMessageTranslateUtil.translateCurrentStateChangeToEvent(clusterName, instanceName,
                  clusterDiffMap.get(instanceName)));
        }
      }
      currentStateDiff.put(clusterName, clusterDiffMap);
    }

    // 2. fetch host health
    for (String clusterName : _livenessCheckEndpointMap.keySet()) {
      for (String instanceName : _livenessCheckEndpointMap.get(clusterName).keySet()) {
        boolean prevLiveness =
            _livenessResults.get(clusterName) != null && _livenessResults.get(clusterName).get(instanceName);
        boolean liveness = fetchInstanceLivenessStatus(clusterName, instanceName);

        if (prevLiveness && !liveness) {  // previously connected, now disconnected
          logger.warn("Host {} is not healthy, sending event to gateway manager", instanceName);
          pushClientEventToGatewayManager(_manager,
              StateTransitionMessageTranslateUtil.translateClientCloseToEvent(clusterName, instanceName));
        } else if (!prevLiveness && liveness) {  // new connection.
          logger.info("Host {} is newly connected, sending init connection event to gateway manager", instanceName);
          pushClientEventToGatewayManager(_manager,
              StateTransitionMessageTranslateUtil.translateCurrentStateDiffToInitConnectEvent(clusterName, instanceName,
                  currentStateDiff.containsKey(clusterName) ? currentStateDiff.get(clusterName).get(instanceName)
                      : new HashMap<>()));
        }
        _livenessResults.computeIfAbsent(clusterName, k -> new HashMap<>()).put(instanceName, liveness);
      }
    }
  }

  @Override
  public void sendStateChangeRequests(String instanceName,
      HelixGatewayServiceOuterClass.ShardChangeRequests shardChangeRequests) {
    switch (_shardStateChannelType) {
      case FILE:
        // we are periodically writing to the file, so no need to write here.
        break;
      default:
        throw new NotImplementedException("Only support file based channel for now");
    }
  }

  @Override
  public void start() throws IOException {
    logger.info("Starting Helix Gateway Service Poll Mode Channel...");
    final Runnable fetchUpdatesTask = new Runnable() {
      @Override
      public void run() {
        fetchUpdates();
      }
    };
    _scheduler.scheduleAtFixedRate(fetchUpdatesTask, _config.getPollStartDelaySec(),  // init delay
        _config.getPollIntervalSec(),            //  poll interval
        TimeUnit.SECONDS);
    scheduleTargetStateUpdateTask();
  }

  void scheduleTargetStateUpdateTask() {
    if (_shardStateChannelType == GatewayServiceChannelConfig.ChannelType.FILE) {
      final Runnable writeTargetStateTask = new Runnable() {
        @Override
        public void run() {
          flushAssignmentToFile(_manager.serializeTargetState(), _targetStateFilePath);
        }
      };
      _scheduler.scheduleAtFixedRate(writeTargetStateTask, _config.getPollStartDelaySec(),  // init delay
          _config.getTargetFileUpdateIntervalSec(),            //  poll interval
          TimeUnit.SECONDS);
    }
  }

  @Override
  public void stop() {
    logger.info("Stopping Helix Gateway Service Poll Mode Channel...");
    // Shutdown the scheduler gracefully when done (e.g., on app termination)
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      _scheduler.shutdown();
      try {
        if (!_scheduler.awaitTermination(1, TimeUnit.MINUTES)) {
          _scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        _scheduler.shutdownNow();
      }
    }));
  }

  @Override
  public void closeConnectionWithError(String clusterName, String instanceName, String reason) {
   // nothing needed for filed based poll mode
  }

  @Override
  public void completeConnection(String clusterName, String instanceName) {
    // nothing needed for filed based poll mode
  }

  /**
   * Get current state of the participants.
   * Now we only support file based, we will add GRPC based in the future.
   */
  protected Map<String, Map<String, Map<String, Map<String, String>>>> getChangedParticipantsCurrentState(
      String userCurrentStateFilePath) {
    Map<String, Map<String, Map<String, Map<String, String>>>> currentShardStates;
    switch (_shardStateChannelType) {
      case FILE:
        currentShardStates = readCurrentStateFromFile(userCurrentStateFilePath);
        return currentShardStates;
      default:
        throw new NotImplementedException("Only support file based channel shard state for now");
    }
  }

  /**
   * Fetch the liveness status of the instance.
   * Now we only support file based, we will add GRPC based in the future.
   */
  protected boolean fetchInstanceLivenessStatus(String clusterName, String instanceName) {
    String endpoint = _livenessCheckEndpointMap.get(clusterName).get(instanceName);
    switch (_participantConnectionStatusChannelType) {
      case FILE:
        return readInstanceLivenessStatusFromFile(endpoint, _config.getPollHealthCheckTimeoutSec());
      default:
        throw new NotImplementedException("Only support grpc based channel for now");
    }
  }
}
