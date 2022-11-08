package org.apache.helix.metaclient.api;

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

public interface ConnectStateChangeListener {

  enum ConnectState {
    // Client is connected to server
    CONNECTED,

    // Authentication failed.
    AUTH_FAILED,

    // Server has expired this connection.
    EXPIRED,

    // When client failed to connect server.
    INIT_FAILED,

    // When client explicitly call disconnect.
    CLOSED_BY_CLIENT
  }


  /**
   * Called when the connection state has changed.
   */
  void handleConnectStateChanged(ConnectState currentState) throws Exception;
  /**
   * Called when new connection is established
   */
  void handleNewConnection(final String sessionId) throws Exception;
  /**
   * Called when new connection failed to established
   */
  void handleConnectionEstablishmentError(final Throwable error) throws Exception;

}