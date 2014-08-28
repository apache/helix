package org.apache.helix.ipc;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import io.netty.buffer.ByteBuf;
import org.apache.helix.resolver.HelixAddress;

import java.util.UUID;

/**
 * Allows message passing among instances in Helix clusters.
 * <p>
 * Messages are sent asynchronously using {@link #send}, and handled by callbacks registered via
 * {@link #registerCallback}
 * </p>
 */
public interface HelixIPCService {

  static final String IPC_PORT = "IPC_PORT";

  /** Starts service (must call before {@link #send}) */
  void start() throws Exception;

  /** Shuts down service and releases any resources */
  void shutdown() throws Exception;

  /** Sends a message to one or more instances that map to a cluster scope. */
  void send(HelixAddress destination, int messageType, UUID messageId, ByteBuf message);

  /** Registers a callback for a given message type */
  void registerCallback(int messageType, HelixIPCCallback callback);
}
