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


import static org.apache.helix.gateway.api.constant.GatewayServiceConfigConstant.*;


public class GatewayServiceChannelConfig {
  public enum ChannelMode {
    PUSH_MODE,
    POLL_MODE
  }

  public enum ChannelType {
    GRPC_SERVER,
    GRPC_CLIENT,
    FILE_SHARE
  }

  private ChannelMode _channelType;

  // service configs
  // channel type for participant liveness detection
  private ChannelType _participantConnectionChannelMode;
  // channel for sending and receiving shard state transition request and shard state response
  private ChannelType _shardStatenChannelMode;

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

  public ChannelMode getChannelMode() {
    return _channelType;
  }
  public ChannelType getParticipantConnectionChannelType() {
    return _participantConnectionChannelMode;
  }

  public ChannelType getShardStatenChannelType() {
    return _shardStatenChannelMode;
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

  private GatewayServiceChannelConfig(int grpcServerPort, ChannelMode channelMode,  ChannelType participantConnectionChannelMode,
      ChannelType shardStatenChannelMode, int serverHeartBeatInterval, int maxAllowedClientHeartBeatInterval,
      int clientTimeout, boolean enableReflectionService, int pollIntervalSec) {
    _grpcServerPort = grpcServerPort;
    _channelType = channelMode;
    _participantConnectionChannelMode = participantConnectionChannelMode;
    _shardStatenChannelMode = shardStatenChannelMode;
    _serverHeartBeatInterval = serverHeartBeatInterval;
    _maxAllowedClientHeartBeatInterval = maxAllowedClientHeartBeatInterval;
    _clientTimeout = clientTimeout;
    _enableReflectionService = enableReflectionService;
    _pollIntervalSec = pollIntervalSec;
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
    // poll mode grpc client configs

    // poll mode file configs


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

    public void validate() {
      if ((_participantConnectionChannelType == ChannelType.GRPC_SERVER || _shardStatenChannelType == ChannelType.GRPC_SERVER) && _grpcServerPort == 0) {
        throw new IllegalArgumentException("Grpc server port must be set for grpc server channel type");
      }
    }

    public GatewayServiceChannelConfig build() {
      validate();
      return new GatewayServiceChannelConfig(_grpcServerPort, _channelMode, _participantConnectionChannelType,
          _shardStatenChannelType, _serverHeartBeatInterval, _maxAllowedClientHeartBeatInterval, _clientTimeout,
          _enableReflectionService, _pollIntervalSec);
    }
  }
}
