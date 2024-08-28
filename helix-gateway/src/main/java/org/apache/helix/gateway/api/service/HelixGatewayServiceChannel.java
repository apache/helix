package org.apache.helix.gateway.api.service;

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
import org.apache.helix.gateway.service.GatewayServiceEvent;
import org.apache.helix.gateway.service.GatewayServiceManager;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass;


/**
 * Helix Gateway Service channel interface provides API for inbound and outbound communication between
 * Gateway service and application instances.
 */
public interface HelixGatewayServiceChannel {

  /**
   * Gateway service send a state transition message to a connected participant.
   *
   * @param instanceName the name of the participant
   */
  void sendStateChangeRequests(String instanceName, HelixGatewayServiceOuterClass.ShardChangeRequests shardChangeRequests);

  /**
   * Send a GatewayServiceEvent to gateway manager for helix instances changes.
   * Event could be a connection closed event (event type DISCONNECT),
   * an initial connection establish event that contains a map of current chard states (event type CONNECT),
   * or a state transition result message (event type UPDATE).
   *
   * The default implementation push an event to the Gateway Service Manager.
   *
   * @param gatewayServiceManager the Gateway Service Manager
   * @param event the event to push
   */
  default void pushClientEventToGatewayManager(GatewayServiceManager gatewayServiceManager, GatewayServiceEvent event) {
    gatewayServiceManager.onGatewayServiceEvent(event);
  }

  /**
   * Start the gateway service channel.
   *
   * @throws IOException if the channel cannot be started
   */
  public void start() throws IOException;

  /**
   * Stop the gateway service channel forcefully.
   */
  public void stop();


  // TODO: remove the following 2 apis in future changes
  /**
   * Gateway service close connection with error. This function is called when manager wants to close client
   * connection when there is an error. e.g. HelixManager connection is lost.
   * @param instanceName  instance name
   * @param reason  reason for closing connection
   */
  public void closeConnectionWithError(String instanceName, String reason);

  /**
   * Gateway service close client connection with success. This function is called when manager wants to close client
   * connection gracefully, e.g., when gateway service is shutting down.
   * @param instanceName  instance name
   */
  public void completeConnection(String instanceName);
}
