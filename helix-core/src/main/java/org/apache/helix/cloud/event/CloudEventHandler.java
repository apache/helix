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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a wrapper around multiple {@link CloudEventListener}, and arrange them by
 * 1. PreEventHandlerCallback -> only one allowed
 * 2. Unordered CloudEventListener list -> multiple allowed
 * 3. PostEventHandlerCallback -> only one allowed
 * to enable an easy management of event listeners and callbacks.
 */
public class CloudEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(CloudEventHandler.class.getName());
  private List<CloudEventListener> _unorderedEventListenerList = new ArrayList<>();
  private CloudEventListener _preEventHandlerCallback = null;
  private CloudEventListener _postEventHandlerCallback = null;

  /**
   * Register an event listener to the event handler. The listeners will be triggered in parallel in events.
   * @param listener
   */
  public void registerCloudEventListener(CloudEventListener listener) {
    _unorderedEventListenerList.add(listener);
  }

  /**
   * Unregister an event listener to the event handler.
   * @param listener
   */
  public void unregisterCloudEventListener(CloudEventListener listener) {
    _unorderedEventListenerList.remove(listener);
  }

  /**
   * Register a callback to be triggered before all the event listeners,
   * only one instance is allowed
   * @param preEventHandlerCallback
   */
  public void registerPreEventHandlerCallback(CloudEventListener preEventHandlerCallback) {
    _preEventHandlerCallback = preEventHandlerCallback;
  }

  /**
   * Unregister the registered preEventHandlerCallback
   * @param preEventHandlerCallback
   */
  public void unregisterPreEventHandlerCallback(CloudEventListener preEventHandlerCallback) {
    _preEventHandlerCallback = null;
  }

  /**
   * Register a callback to be triggered after all the event listeners,
   * only one instance is allowed
   * @param postEventHandlerCallback
   */
  public void registerPostEventHandlerCallback(CloudEventListener postEventHandlerCallback) {
    _postEventHandlerCallback = postEventHandlerCallback;
  }

  /**
   * Unregister the registered postEventHandlerCallback
   * @param postEventHandlerCallback
   */
  public void unregisterPostEventHandlerCallback(
      CloudEventListener postEventHandlerCallback) {
    _postEventHandlerCallback = null;
  }

  /**
   * Trigger the callback / listeners in order of
   * 1. PreEventHandlerCallback
   * 2. Unordered CloudEventListener list
   * 3. PostEventHandlerCallback
   * @param eventInfo the object contains any information about the incoming event
   */
  public void onPause(Object eventInfo) {
    if (_preEventHandlerCallback != null) {
      _preEventHandlerCallback.onPause(eventInfo);
    }
    _unorderedEventListenerList.forEach(listener -> listener.onPause(eventInfo));
    if (_postEventHandlerCallback != null) {
      _postEventHandlerCallback.onPause(eventInfo);
    }
  }

  /**
   * Trigger the callback / listeners in order of
   * 1. PreEventHandlerCallback
   * 2. Unordered CloudEventListener list
   * 3. PostEventHandlerCallback
   * @param eventInfo the object contains any information about the incoming event
   */
  public void onResume(Object eventInfo) {
    if (_preEventHandlerCallback != null) {
      _preEventHandlerCallback.onResume(eventInfo);
    }
    _unorderedEventListenerList.forEach(listener -> listener.onResume(eventInfo));
    if (_postEventHandlerCallback != null) {
      _postEventHandlerCallback.onResume(eventInfo);
    }
  }
}
