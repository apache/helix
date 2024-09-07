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

import java.util.Map;
import java.util.Properties;

import static org.apache.helix.gateway.api.constant.GatewayServiceConfigConstant.*;
import static org.apache.helix.gateway.channel.GatewayServiceChannelConfig.FileBasedConfigType.*;


public class GatewayServiceChannelConfig {
  // Mode to get helix participant information (inbound information). This included health check and shard state transition response
  // We do not support hybrid mode as of now, (i.e. have push mode for participant liveness detection and pull mode for shard state)
  public enum ChannelMode {
    PUSH_MODE, // The gateway service passively receives participant information
    POLL_MODE  // The gateway service actively polls participant information
  }

  // NOTE:
  // For outbound information - stateTransition request, Gateway service will always push the state transition message.
  // We do not support participant poll mode for stateTransition request as of now.

  // channel type for the following 3 information - participant liveness detection, shard state transition request and response
  // By default, they are all grpc server, user could define them separately.
  public enum ChannelType {
    GRPC_SERVER,
    GRPC_CLIENT,
    FILE
  }

  // service configs

  // service mode for inbound information.
  private final ChannelMode _channelMode;
  // channel type for participant liveness detection
  private final ChannelType _participantConnectionChannelType;
  // channel for sending and receiving shard state transition request and shard state response
  private final ChannelType _shardStateChannelType;

  // grpc server configs
  private final int _grpcServerPort;
  private final int _serverHeartBeatInterval;
  private final int _maxAllowedClientHeartBeatInterval;
  private final int _clientTimeout;
  private final boolean _enableReflectionService;

  // poll mode config
  private final int _pollIntervalSec;
  private final int _pollStartDelaySec;
  private final int _pollHealthCheckTimeoutSec;
  private final int _targetFileUpdateIntervalSec;
  private final Map<String, Map<String, String>> _participantLivenessEndpointMap;
  private final Properties _pollModeConfigs;

  public enum FileBasedConfigType {
    PARTICIPANT_CURRENT_STATE_PATH,
    SHARD_TARGET_STATE_PATH
  }

  // getters
  public ChannelMode getChannelMode() {
    return _channelMode;
  }

  public ChannelType getParticipantConnectionChannelType() {
    return _participantConnectionChannelType;
  }

  public ChannelType getShardStateChannelType() {
    return _shardStateChannelType;
  }

  public int getGrpcServerPort() {
    return _grpcServerPort;
  }

  public int getServerHeartBeatInterval() {
    return _serverHeartBeatInterval;
  }

  public int getMaxAllowedClientHeartBeatInterval() {
    return _maxAllowedClientHeartBeatInterval;
  }

  public int getClientTimeout() {
    return _clientTimeout;
  }

  public boolean getEnableReflectionService() {
    return _enableReflectionService;
  }

  public int getPollIntervalSec() {
    return _pollIntervalSec;
  }

  public Map<String, Map<String, String>> getParticipantLivenessEndpointMap() {
    return _participantLivenessEndpointMap;
  }

  public int getPollStartDelaySec() {
    return _pollStartDelaySec;
  }

  public int getPollHealthCheckTimeoutSec() {
    return _pollHealthCheckTimeoutSec;
  }

  public int getTargetFileUpdateIntervalSec() {
    return _targetFileUpdateIntervalSec;
  }

  public String getPollModeConfig(FileBasedConfigType type) {
    return _pollModeConfigs.getProperty(type.toString());
  }

  private GatewayServiceChannelConfig(int grpcServerPort, ChannelMode channelMode,
      ChannelType participantConnectionChannelType, ChannelType shardStateChannelType, int serverHeartBeatInterval,
      int maxAllowedClientHeartBeatInterval, int clientTimeout, boolean enableReflectionService, int pollIntervalSec,
      int pollStartDelaySec, int pollHealthCheckTimeoutSec, int targetFileUpdateIntervalSec,
      Properties pollModeConfigs, Map<String, Map<String, String>> participantLivenessEndpointMap) {
    _grpcServerPort = grpcServerPort;
    _channelMode = channelMode;
    _participantConnectionChannelType = participantConnectionChannelType;
    _shardStateChannelType = shardStateChannelType;
    _serverHeartBeatInterval = serverHeartBeatInterval;
    _maxAllowedClientHeartBeatInterval = maxAllowedClientHeartBeatInterval;
    _clientTimeout = clientTimeout;
    _enableReflectionService = enableReflectionService;
    _pollIntervalSec = pollIntervalSec;
    _pollStartDelaySec = pollStartDelaySec;
    _pollHealthCheckTimeoutSec = pollHealthCheckTimeoutSec;
    _targetFileUpdateIntervalSec = targetFileUpdateIntervalSec;
    _pollModeConfigs = pollModeConfigs;
    _participantLivenessEndpointMap = participantLivenessEndpointMap;
  }

  public static class GatewayServiceProcessorConfigBuilder {

    // service configs
    private ChannelMode _channelMode = ChannelMode.PUSH_MODE;
    private ChannelType _participantConnectionChannelType = ChannelType.GRPC_SERVER;
    private ChannelType _shardStatenChannelType = ChannelType.GRPC_SERVER;

    // grpc server configs
    private int _grpcServerPort;
    private int _serverHeartBeatInterval = DEFAULT_SERVER_HEARTBEAT_INTERVAL;
    private int _maxAllowedClientHeartBeatInterval = DEFAULT_AMX_ALLOWED_CLIENT_HEARTBEAT_INTERVAL;
    private int _clientTimeout = DEFAULT_CLIENT_TIMEOUT;
    private boolean _enableReflectionService = true;

    // poll mode config
    private int _pollIntervalSec = DEFAULT_POLL_INTERVAL_SEC;
    // poll mode config
    private Properties _pollModeConfigs;
    private int _pollStartDelaySec = DEFAULT_POLL_INTERVAL_SEC;
    private int _pollHealthCheckTimeoutSec = DEFAULT_HEALTH_TIMEOUT_SEC;
    private int _targetFileUpdateIntervalSec = DEFAULT_POLL_INTERVAL_SEC;
    private Map<String, Map<String, String>> _healthCheckEndpointMap;

    public GatewayServiceProcessorConfigBuilder setChannelMode(ChannelMode channelMode) {
      _channelMode = channelMode;
      return this;
    }

    public GatewayServiceProcessorConfigBuilder setParticipantConnectionChannelType(ChannelType channelMode) {
      _participantConnectionChannelType = channelMode;
      return this;
    }

    public GatewayServiceProcessorConfigBuilder setShardStateProcessorType(ChannelType channelMode) {
      _shardStatenChannelType = channelMode;
      return this;
    }

    public GatewayServiceProcessorConfigBuilder setGrpcServerPort(int grpcServerPort) {
      _grpcServerPort = grpcServerPort;
      return this;
    }

    public GatewayServiceProcessorConfigBuilder setServerHeartBeatInterval(int serverHeartBeatInterval) {
      _serverHeartBeatInterval = serverHeartBeatInterval;
      return this;
    }

    public GatewayServiceProcessorConfigBuilder setMaxAllowedClientHeartBeatInterval(
        int maxAllowedClientHeartBeatInterval) {
      _maxAllowedClientHeartBeatInterval = maxAllowedClientHeartBeatInterval;
      return this;
    }

    public GatewayServiceProcessorConfigBuilder setClientTimeout(int clientTimeout) {
      _clientTimeout = clientTimeout;
      return this;
    }

    public GatewayServiceProcessorConfigBuilder setEnableReflectionService(boolean enableReflectionService) {
      _enableReflectionService = enableReflectionService;
      return this;
    }

    public GatewayServiceProcessorConfigBuilder setPollIntervalSec(int pollIntervalSec) {
      _pollIntervalSec = pollIntervalSec;
      return this;
    }

    public GatewayServiceProcessorConfigBuilder addPollModeConfig(FileBasedConfigType type, String value) {
      if (_pollModeConfigs == null) {
        _pollModeConfigs = new Properties();
      }
      _pollModeConfigs.put(type.toString(), value);
      return this;
    }

    public GatewayServiceProcessorConfigBuilder setPollStartDelaySec(int pollStartDelaySec) {
      _pollStartDelaySec = pollStartDelaySec;
      return this;
    }

    public GatewayServiceProcessorConfigBuilder setPollHealthCheckTimeout(int pollHealthCheckTimeout) {
      _pollHealthCheckTimeoutSec = pollHealthCheckTimeout;
      return this;
    }

    public GatewayServiceProcessorConfigBuilder setTargetFileUpdateIntervalSec(int targetFileUpdateIntervalSec) {
      _targetFileUpdateIntervalSec = targetFileUpdateIntervalSec;
      return this;
    }

    public GatewayServiceProcessorConfigBuilder setHealthCheckEndpointMap(Map<String, Map<String, String>> healthCheckEndpointMap) {
      _healthCheckEndpointMap = healthCheckEndpointMap;
      return this;
    }

    public void validate() {
      switch (_participantConnectionChannelType) {
        case GRPC_SERVER:
          if (_grpcServerPort == 0) {
            throw new IllegalArgumentException("Grpc server port must be set for grpc server channel type");
          }
          if (_shardStatenChannelType != ChannelType.GRPC_SERVER) {
            throw new IllegalArgumentException(
                "In case of GRPC server, Participant connection channel type and shard state channel type must be the same");
          }
          break;
        case FILE:
          if (_healthCheckEndpointMap == null || _healthCheckEndpointMap.isEmpty()) {
            throw new IllegalArgumentException("Health check endpoint map must be set for file channel type");
          }
          break;
        default:
          break;
      }

      switch (_shardStatenChannelType) {
        case GRPC_SERVER:
          if (_participantConnectionChannelType != ChannelType.GRPC_SERVER) {
            throw new IllegalArgumentException(
                "In case of GRPC server, Participant connection channel type and shard state channel type must be the same");
          }
          break;
        case FILE:
          if (_pollModeConfigs == null || _pollModeConfigs.getProperty(SHARD_TARGET_STATE_PATH.name()) == null
              || _pollModeConfigs.getProperty(SHARD_TARGET_STATE_PATH.name()).isEmpty()) {
            throw new IllegalArgumentException("Current state and target state path must be set for file channel type");
          }
          break;
        default:
          break;
      }
    }

    public GatewayServiceChannelConfig build() {
      validate();
      return new GatewayServiceChannelConfig(_grpcServerPort, _channelMode, _participantConnectionChannelType,
          _shardStatenChannelType, _serverHeartBeatInterval, _maxAllowedClientHeartBeatInterval, _clientTimeout,
          _enableReflectionService, _pollIntervalSec, _pollStartDelaySec, _pollHealthCheckTimeoutSec,
          _targetFileUpdateIntervalSec, _pollModeConfigs, _healthCheckEndpointMap);
    }
  }
}
