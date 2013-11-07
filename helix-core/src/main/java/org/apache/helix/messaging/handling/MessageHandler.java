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
 *   http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * Provides the base class for all message handlers.
 */
public abstract class MessageHandler {
  public enum ErrorType {
    FRAMEWORK,
    INTERNAL
  }

  public enum ErrorCode {
    ERROR,
    CANCEL,
    TIMEOUT
  }

  /**
   * The message to be handled
   */
  protected final Message _message;

  /**
   * The context for handling the message. The cluster manager interface can be
   * accessed from NotificationContext
   */
  protected final NotificationContext _notificationContext;

  /**
   * The constructor. The message and notification context must be provided via
   * creation.
   */
  public MessageHandler(Message message, NotificationContext context) {
    _message = message;
    _notificationContext = context;
  }

  /**
   * Message handling routine. The function is called in a thread pool task in
   * CMTaskExecutor
   * @return returns the CMTaskResult which contains info about the message processing.
   */
  public abstract HelixTaskResult handleMessage() throws InterruptedException;

  /**
   * Callback when error happens in the message handling pipeline.
   * @param type TODO
   * @param retryCountLeft - The number of retries that the framework will
   *          continue trying to handle the message
   * @param ErrorType - denote if the exception happens in framework or happens in the
   *          customer's code
   */
  public abstract void onError(Exception e, ErrorCode code, ErrorType type);

  /**
   * Callback when the framework is about to interrupt the message handler
   */
  public void onTimeout() {

  }
}
