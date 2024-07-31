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

import io.grpc.Server;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.helix.gateway.grpcservice.HelixGatewayServiceGrpcService;
import org.apache.helix.gateway.service.GatewayServiceManager;
import org.apache.helix.gateway.util.HelixGatewayGrpcServerBuilder;


/**
 * Main class for Helix Gateway.
 * It starts the Helix Gateway grpc service.
 */
public final class HelixGatewayMain {

  private HelixGatewayMain() {
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    // Create a new server to listen on port 50051
    GatewayServiceManager manager = new GatewayServiceManager();
    Server server = new HelixGatewayGrpcServerBuilder().setPort(50051)
        .setGrpcService((HelixGatewayServiceGrpcService)manager.getHelixGatewayServiceProcessor())
        .build();

    server.start();
    System.out.println("Server started, listening on " + server.getPort());

    // Wait for the server to shutdown
    server.awaitTermination(365, TimeUnit.DAYS);
  }
}

