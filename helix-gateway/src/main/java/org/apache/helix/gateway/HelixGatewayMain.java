package org.apache.helix.gateway;

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
import org.apache.helix.gateway.service.GatewayServiceManager;
import org.apache.helix.gateway.channel.GatewayServiceChannelConfig;

import static java.lang.Integer.*;


/**
 * Main class for Helix Gateway.
 * It starts the Helix Gateway grpc service.
 * args0: zk address
 * args1: helix gateway groc server port
 */
public final class HelixGatewayMain {

  private HelixGatewayMain() {
  }

  public static void main(String[] args) throws IOException {
    // Create a new server
    GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder builder =
        new GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder();
    GatewayServiceManager manager =
        new GatewayServiceManager(args[0], builder.setGrpcServerPort(parseInt(args[1])).build());

    manager.startService();
  }
}

