package org.apache.helix.cloud.event;

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

/**
 * This class is a basic unit for callbacks to handle events.
 * The listeners can be registered to {@link CloudEventHandler}.
 */
public interface CloudEventListener {
  /**
   * Defines different listener types
   * It determines the position this listener being triggered in {@link CloudEventHandler}
   * Order being: PRE_EVENT_HANDLER -> UNORDERED (in parallel) -> POST_EVENT_HANDLER
   */
  enum ListenerType {
    PRE_EVENT_HANDLER,
    UNORDERED,
    POST_EVENT_HANDLER
  }

  /**
   * Perform action to react the the event
   * @param eventType Type of the event
   * @param eventInfo Detailed information about the event
   */
  void performAction(Object eventType, Object eventInfo);

  /**
   * Get the listener type of a listener
   * @return The type of the listener
   */
  ListenerType getListenerType();
}
