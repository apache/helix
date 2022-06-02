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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a wrapper around multiple {@link CloudEventListener}, and arrange them by
 * 1. PreEventHandlerCallback -> only one allowed
 * 2. Unordered CloudEventListener list -> multiple allowed
 * 3. PostEventHandlerCallback -> only one allowed
 * to enable an easy management of event listeners and callbacks.
 */
public class CloudEventHandler implements AbstractEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(CloudEventHandler.class.getName());
  private List<CloudEventListener> _unorderedEventListenerList = new ArrayList<>();
  private Optional<CloudEventListener> _preEventHandlerCallback = Optional.empty();
  private Optional<CloudEventListener> _postEventHandlerCallback = Optional.empty();

  /**
   * Register an event listener to the event handler.
   * If no listener type is specified, register as an unordered listener.
   * @param listener
   */
  @Override
  public void registerCloudEventListener(CloudEventListener listener) {
    if (listener != null) {
      switch (listener.getListenerType()) {
        case PRE_EVENT_HANDLER:
          _preEventHandlerCallback = Optional.of(listener);
          break;
        case POST_EVENT_HANDLER:
          _postEventHandlerCallback = Optional.of(listener);
          break;
        case UNORDERED:
        default:
          _unorderedEventListenerList.add(listener);
          break;
      }
    }
  }

  /**
   * Unregister an event listener to the event handler.
   * @param listener
   */
  @Override
  public void unregisterCloudEventListener(CloudEventListener listener) {
    _unorderedEventListenerList.remove(listener);
  }

  /**
   * Trigger the registered listeners in order,
   * and trigger the corresponding callback registered in the listeners for a certain type of event.
   */
  public void performAction(Object eventType, Object eventInfo) {
    _preEventHandlerCallback.ifPresent(callback -> callback.performAction(eventType, eventInfo));
    _unorderedEventListenerList.parallelStream().forEach(listener -> listener.performAction(eventType, eventInfo));
    _postEventHandlerCallback.ifPresent(callback -> callback.performAction(eventType, eventInfo));
  }
}
