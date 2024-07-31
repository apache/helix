package org.apache.helix.gateway.util;

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

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.util.concurrent.TimeUnit;

import static org.apache.helix.gateway.constant.GatewayServiceGrpcDefaultConfig.*;


/**
   * Builder class to create a Helix gateway service server with custom configurations.
   */
  public  class HelixGatewayGrpcServerBuilder {
    private int port;
    private BindableService service;
    private int serverHeartBeatInterval = DEFAULT_SERVER_HEARTBEAT_INTERVAL;
    private int maxAllowedClientHeartBeatInterval = DEFAULT_AMX_ALLOWED_CLIENT_HEARTBEAT_INTERVAL;
    private int clientTimeout = DEFAULT_CLIENT_TIMEOUT;
    private boolean enableReflectionService = true;

    public HelixGatewayGrpcServerBuilder setPort(int port) {
      this.port = port;
      return this;
    }

    public HelixGatewayGrpcServerBuilder setServerHeartBeatInterval(int serverHeartBeatInterval) {
      this.serverHeartBeatInterval = serverHeartBeatInterval;
      return this;
    }

    public HelixGatewayGrpcServerBuilder setMaxAllowedClientHeartBeatInterval(int maxAllowedClientHeartBeatInterval) {
      this.maxAllowedClientHeartBeatInterval = maxAllowedClientHeartBeatInterval;
      return this;
    }

    public HelixGatewayGrpcServerBuilder setClientTimeout(int clientTimeout) {
      this.clientTimeout = clientTimeout;
      return this;
    }

    public HelixGatewayGrpcServerBuilder setGrpcService(BindableService service) {
      this.service = service;
      return this;
    }

    public HelixGatewayGrpcServerBuilder enableReflectionService(boolean enableReflectionService) {
      this.enableReflectionService = enableReflectionService;
      return this;
    }

    public Server build() {
      validate();

      ServerBuilder serverBuilder = ServerBuilder.forPort(port)
          .addService(service)
          .keepAliveTime(serverHeartBeatInterval, TimeUnit.SECONDS)  // HeartBeat time
          .keepAliveTimeout(clientTimeout, TimeUnit.SECONDS)  // KeepAlive client timeout
          .permitKeepAliveTime(maxAllowedClientHeartBeatInterval, TimeUnit.SECONDS)  // Permit min HeartBeat time
          .permitKeepAliveWithoutCalls(true);  // Allow KeepAlive forever without active RPCs

      if (enableReflectionService) {
        serverBuilder.addService(ProtoReflectionService.newInstance());
      }

      return serverBuilder
          .build();
    }

    private void validate() {
      if (port == 0 || service == null) {
        throw new IllegalArgumentException("Port and service must be set");
      }
      if (clientTimeout < maxAllowedClientHeartBeatInterval) {
        throw new IllegalArgumentException("Client timeout is less than max allowed client heartbeat interval");
      }
    }
  }

