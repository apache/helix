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

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.gateway.service.GatewayServiceEvent;
import org.apache.helix.gateway.service.GatewayServiceManager;
import org.apache.helix.gateway.api.service.HelixGatewayServiceChannel;
import org.apache.helix.gateway.util.PerKeyLockRegistry;
import org.apache.helix.gateway.util.StateTransitionMessageTranslateUtil;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proto.org.apache.helix.gateway.HelixGatewayServiceGrpc;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardState;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardStateMessage;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.TransitionMessage;


/**
 * Helix Gateway Service GRPC UI implementation.
 */
public class HelixGatewayServiceGrpcService extends HelixGatewayServiceGrpc.HelixGatewayServiceImplBase
    implements HelixGatewayServiceChannel {
  // create LOGGER
  private static final Logger logger = LoggerFactory.getLogger(HelixGatewayServiceGrpcService.class);

  // Map to store the observer for each instance
  private final Map<String, StreamObserver<TransitionMessage>> _observerMap = new HashMap<>();
  // A reverse map to store the instance name for each observer. It is used to find the instance when connection is closed.
  // map<observer, pair<instance, cluster>>
  private final Map<StreamObserver<TransitionMessage>, Pair<String, String>> _reversedObserverMap = new HashMap<>();

  private final GatewayServiceManager _manager;

  // A fine grain lock register on instance level
  private final PerKeyLockRegistry _lockRegistry;

  private final GatewayServiceChannelConfig _config;

  private Server _server;

  public HelixGatewayServiceGrpcService(GatewayServiceManager manager, GatewayServiceChannelConfig config) {
    _manager = manager;
    _config = config;
    _lockRegistry = new PerKeyLockRegistry();
  }

  /**
   * Grpc service end pint.
   * Application instances Report the state of the shard or result of transition request to the gateway service.
   *
   * @param responseObserver the observer to send the response to the client
   * @return the observer to receive the state of the shard or result of transition request
   */
  @Override
  public StreamObserver<proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardStateMessage> report(
      StreamObserver<proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.TransitionMessage> responseObserver) {

    return new StreamObserver<ShardStateMessage>() {

      @Override
      public void onNext(ShardStateMessage request) {
        logger.info("Receive message from instance: {}", request.toString());
        if (request.hasShardState()) {
          ShardState shardState = request.getShardState();
          updateObserver(shardState.getInstanceName(), shardState.getClusterName(), responseObserver);
        }
        pushClientEventToGatewayManager(_manager,
            StateTransitionMessageTranslateUtil.translateShardStateMessageToEvent(request));
      }

      @Override
      public void onError(Throwable t) {
        logger.info("Receive on error, reason: {} message: {}", Status.fromThrowable(t).getCode(), t.getMessage());
        Pair<String, String> instanceInfo = _reversedObserverMap.get(responseObserver);
        onClientClose(instanceInfo.getRight(), instanceInfo.getLeft());
      }

      @Override
      public void onCompleted() {
        logger.info("Receive on complete message");
        Pair<String, String> instanceInfo = _reversedObserverMap.get(responseObserver);
        onClientClose(instanceInfo.getRight(), instanceInfo.getLeft());
      }
    };
  }

  /**
   * Send state transition message to the instance.
   * The instance must already have established a connection to the gateway service.
   *
   * @param instanceName the instance name to send the message to
   * @param currentState the current state of shard
   * @param message the message to convert to the transition message
   */
  @Override
  public void sendStateTransitionMessage(String instanceName, String currentState, Message message) {
    StreamObserver<TransitionMessage> observer;
    observer = _observerMap.get(instanceName);
    if (observer != null) {
      observer.onNext(StateTransitionMessageTranslateUtil.translateSTMsgToTransitionMessage(message));
    }
  }

  /**
   * Close the connection of the instance. If closed because of error, use the error reason to close the connection.
   * @param instanceName instance name
   * @param errorReason   error reason for close
   */
  @Override
  public void closeConnectionWithError(String instanceName, String errorReason) {
    logger.info("Close connection for instance: {} with error reason: {}", instanceName, errorReason);
    closeConnectionHelper(instanceName, errorReason, true);
  }

  /**
   * Complete the connection of the instance.
   * @param instanceName instance name
   */
  @Override
  public void completeConnection(String instanceName) {
    logger.info("Complete connection for instance: {}", instanceName);
    closeConnectionHelper(instanceName, null, false);
  }

  private void closeConnectionHelper(String instanceName, String errorReason, boolean withError) {
    StreamObserver<TransitionMessage> observer;
    observer = _observerMap.get(instanceName);
    if (observer != null) {
      if (withError) {
        observer.onError(Status.UNAVAILABLE.withDescription(errorReason).asRuntimeException());
      } else {
        observer.onCompleted();
      }
    }
  }

  public void onClientClose(String clusterName, String instanceName) {
    if (instanceName == null || clusterName == null) {
      // TODO: log error;
      return;
    }
    logger.info("Client close connection for instance: {}", instanceName);
    GatewayServiceEvent event =
        StateTransitionMessageTranslateUtil.translateClientCloseToEvent(clusterName, instanceName);
    pushClientEventToGatewayManager(_manager, event);
    _lockRegistry.withLock(instanceName, () -> {
      _reversedObserverMap.remove(_observerMap.get(instanceName));
      _observerMap.remove(instanceName);
      _lockRegistry.removeLock(instanceName);
    });
  }

  private void updateObserver(String instanceName, String clusterName,
      StreamObserver<TransitionMessage> streamObserver) {
    _lockRegistry.withLock(instanceName, () -> {
      _observerMap.put(instanceName, streamObserver);
      _reversedObserverMap.put(streamObserver, new ImmutablePair<>(instanceName, clusterName));
    });
  }

  @Override
  public void start() throws IOException {
    ServerBuilder serverBuilder = ServerBuilder.forPort(_config.getGrpcServerPort())
        .addService(this)
        .keepAliveTime(_config.getServerHeartBeatInterval(),
            TimeUnit.SECONDS)  // HeartBeat time
        .keepAliveTimeout(_config.getClientTimeout(),
            TimeUnit.SECONDS)  // KeepAlive client timeout
        .permitKeepAliveTime(_config.getMaxAllowedClientHeartBeatInterval(),
            TimeUnit.SECONDS)  // Permit min HeartBeat time
        .permitKeepAliveWithoutCalls(true);  // Allow KeepAlive forever without active RPC
    if (_config.getEnableReflectionService()) {
      serverBuilder = serverBuilder.addService(io.grpc.protobuf.services.ProtoReflectionService.newInstance());
    }
    _server = serverBuilder.build();

    logger.info("Starting grpc server on port " + _config.getGrpcServerPort() + " now.... Server heart beat interval: "
        + _config.getServerHeartBeatInterval() + " seconds, Max allowed client heart beat interval: "
        + _config.getMaxAllowedClientHeartBeatInterval() + " seconds, Client timeout: " + _config.getClientTimeout()
        + " seconds, Enable reflection service: " + _config.getEnableReflectionService());
    _server.start();
  }

  @Override
  public void stop() {
    if (_server != null) {
      logger.info("Shutting down grpc server now....");
      _server.shutdownNow();
    }
  }

  @VisibleForTesting
  Server getServer() {
    return _server;
  }
}
