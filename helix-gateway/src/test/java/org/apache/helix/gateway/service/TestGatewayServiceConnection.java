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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.helix.gateway.base.HelixGatewayTestBase;
import org.apache.helix.gateway.channel.GatewayServiceChannelConfig;
import org.apache.helix.gateway.api.constant.GatewayServiceEventType;
import org.testng.Assert;
import org.testng.annotations.Test;
import proto.org.apache.helix.gateway.HelixGatewayServiceGrpc;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass;



public class TestGatewayServiceConnection extends HelixGatewayTestBase {
  CountDownLatch connectLatch = new CountDownLatch(1);
  CountDownLatch disconnectLatch = new CountDownLatch(1);

  @Test
  public void TestLivenessDetection() throws IOException, InterruptedException {
    // start the gateway service
    GatewayServiceChannelConfig config =
        new GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder().setGrpcServerPort(50051).build();
    GatewayServiceManager mng = new DummyGatewayServiceManager(config);
    mng.startService();

    // start the client
    HelixGatewayClient client = new HelixGatewayClient("localhost", 50051);
    client.connect();
    // assert we get connect event
    Assert.assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

    // assert we have disconnect event
    client.shutdownGracefully();
    Assert.assertTrue(disconnectLatch.await(5, TimeUnit.SECONDS));

    connectLatch = new CountDownLatch(1);
    disconnectLatch = new CountDownLatch(1);

    // start the client
    HelixGatewayClient client2 = new HelixGatewayClient("localhost", 50051);
    client2.connect();
    // assert we get connect event
    Assert.assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

    // assert we have disconnect event when shutting down ungracefully
    client2.shutdownByClosingConnection();
    Assert.assertTrue(disconnectLatch.await(5, TimeUnit.SECONDS));

    mng.stopService();
  }

  public class HelixGatewayClient {

    private final ManagedChannel channel;
    private final HelixGatewayServiceGrpc.HelixGatewayServiceStub asyncStub;

    private StreamObserver _requestObserver;

    public HelixGatewayClient(String host, int port) {
      this.channel = ManagedChannelBuilder.forAddress(host, port)
          .usePlaintext()
          .keepAliveTime(30, TimeUnit.SECONDS)  // KeepAlive time
          .keepAliveTimeout(3, TimeUnit.MINUTES)  // KeepAlive timeout
          .keepAliveWithoutCalls(true)  // Allow KeepAlive without active RPCs
          .build();
      this.asyncStub = HelixGatewayServiceGrpc.newStub(channel);
    }

    public void connect() {
      _requestObserver = asyncStub.report(new StreamObserver<HelixGatewayServiceOuterClass.ShardChangeRequests>() {
        @Override
        public void onNext(HelixGatewayServiceOuterClass.ShardChangeRequests value) {
          // Handle response from server
        }

        @Override
        public void onError(Throwable t) {
          // Handle error
        }

        @Override
        public void onCompleted() {
          // Handle stream completion
        }
      });

      // Send initial ShardStateMessage
      HelixGatewayServiceOuterClass.ShardStateMessage initialMessage =
          HelixGatewayServiceOuterClass.ShardStateMessage.newBuilder()
              .setShardState(HelixGatewayServiceOuterClass.ShardState.newBuilder()
                  .setInstanceName("instance1")
                  .setClusterName("TEST_CLUSTER")
                  .build())
              .build();
      _requestObserver.onNext(initialMessage);

      // Add more logic to send additional messages if needed
    }

    public void shutdownGracefully() {
      // graceful shutdown
      _requestObserver.onCompleted();
      channel.shutdown().shutdownNow();
    }

    public void shutdownByClosingConnection() {
      //ungraceful shutdown
      channel.shutdown().shutdownNow();
    }
  }

  class DummyGatewayServiceManager extends GatewayServiceManager {

    public DummyGatewayServiceManager(GatewayServiceChannelConfig gatewayServiceChannelConfig) {
      super("dummyZkAddress", gatewayServiceChannelConfig);
    }

    @Override
    public void onGatewayServiceEvent(GatewayServiceEvent event) {
      if (event.getEventType().equals(GatewayServiceEventType.CONNECT)) {
        connectLatch.countDown();
      } else if (event.getEventType().equals(GatewayServiceEventType.DISCONNECT)) {
        disconnectLatch.countDown();
      }
      System.out.println("Received event: " + event.getEventType());
    }
  }
}
