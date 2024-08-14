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


import static org.apache.helix.gateway.api.constant.GatewayServiceDefaultConfig.*;

public class GatewayServiceChannelConfig {
  public enum ChannelType {
    GRPC_SERVER, POLL_GRPC, SHARED_FILE
  }

  // service configs
  // channel type for participant liveness detection
  private ChannelType _participantConnectionChannelType;
  // channel for sending and receiving shard state transition request and shard state response
  private ChannelType _shardStatenChannelType;

  // grpc server configs
  private final int _grpcServerPort;
  private final int _serverHeartBeatInterval;
  private final int _maxAllowedClientHeartBeatInterval;
  private final int _clientTimeout;
  private final boolean _enableReflectionService;

  // poll mode config
  private final int _pollIntervalSec;
  // TODO: configs for pull mode grpc client

  // TODO: configs for pull mode with file

  // getters
  public ChannelType getParticipantConnectionChannelType() {
    return _participantConnectionChannelType;
  }

  public ChannelType getShardStatenChannelType() {
    return _shardStatenChannelType;
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

  public GatewayServiceChannelConfig(int grpcServerPort, ChannelType participantConnectionChannelType,
      ChannelType shardStatenChannelType, int serverHeartBeatInterval, int maxAllowedClientHeartBeatInterval,
      int clientTimeout, boolean enableReflectionService, int pollIntervalSec) {
    _grpcServerPort = grpcServerPort;
    _participantConnectionChannelType = participantConnectionChannelType;
    _shardStatenChannelType = shardStatenChannelType;
    _serverHeartBeatInterval = serverHeartBeatInterval;
    _maxAllowedClientHeartBeatInterval = maxAllowedClientHeartBeatInterval;
    _clientTimeout = clientTimeout;
    _enableReflectionService = enableReflectionService;
    _pollIntervalSec = pollIntervalSec;
  }

  public static class GatewayServiceProcessorConfigBuilder {

    // service configs
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
    // poll mode grpc client configs

    // poll mode file configs

    public GatewayServiceProcessorConfigBuilder setParticipantConnectionChannelType(ChannelType channelType) {
      _participantConnectionChannelType = channelType;
      return this;
    }

    public GatewayServiceProcessorConfigBuilder setShardStateProcessorType(ChannelType channelType) {
      _shardStatenChannelType = channelType;
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

    public void validate() {
      if ((_participantConnectionChannelType == ChannelType.POLL_GRPC
          && _shardStatenChannelType != ChannelType.POLL_GRPC) || (
          _participantConnectionChannelType != ChannelType.POLL_GRPC
              && _shardStatenChannelType == ChannelType.POLL_GRPC)) {
        throw new IllegalArgumentException(
            "Unsupported channel type config: ConnectionChannelType: " + _participantConnectionChannelType
                + " shardStatenChannelType: " + _shardStatenChannelType);
      }
    }

    public GatewayServiceChannelConfig build() {
      validate();
      return new GatewayServiceChannelConfig(_grpcServerPort, _participantConnectionChannelType,
          _shardStatenChannelType, _serverHeartBeatInterval, _maxAllowedClientHeartBeatInterval, _clientTimeout,
          _enableReflectionService, _pollIntervalSec);
    }
  }
}
