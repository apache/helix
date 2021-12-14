package org.apache.helix.messaging.handling;

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

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public interface MessageHandlerFactory {

  /**
   * Create the message handler for processing the message task.
   * @param message
   * @param context
   * @return message handler object.
   *         Or null if the message cannot be processed given the current status.
   */
  MessageHandler createHandler(Message message, NotificationContext context);

  @Deprecated
  // Please update the logic to implement MultiTypeMessageHandlerFactory.getMessageTypes instead.
  String getMessageType();

  void reset();

  default void sync() {
    LogHolder.LOG.warn("Invoked default sync() without any operation");
  }
}

final class LogHolder {
  static final Logger LOG = LoggerFactory.getLogger(MessageHandlerFactory.class);
}
