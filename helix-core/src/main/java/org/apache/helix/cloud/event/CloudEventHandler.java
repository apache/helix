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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(CloudEventHandler.class.getName());
  @VisibleForTesting
  protected List<CloudEventListener> _eventListeners = new ArrayList<>();
  private PreEventHandlerCallback _preEventHandlerCallback = null;
  private PostEventHandlerCallback _postEventHandlerCallback = null;

  public void registerCloudEventListener(CloudEventListener listener) {
    _eventListeners.add(listener);
  }

  public void unregisterCloudEventListener(CloudEventListener listener) {
    _eventListeners.remove(listener);
  }

  public void registerPreEventHandlerCallback(PreEventHandlerCallback preEventHandlerCallback) {
    _preEventHandlerCallback = preEventHandlerCallback;
  }

  public void unregisterPreEventHandlerCallback(PreEventHandlerCallback preEventHandlerCallback) {
    _preEventHandlerCallback = null;
  }

  public PreEventHandlerCallback getPreEventHandlerCallback() {
    return _preEventHandlerCallback;
  }

  public PostEventHandlerCallback getPostEventHandlerCallback() {
    return _postEventHandlerCallback;
  }

  public void registerPostEventHandlerCallback(PostEventHandlerCallback postEventHandlerCallback) {
    _postEventHandlerCallback = postEventHandlerCallback;
  }

  public void unregisterPostEventHandlerCallback(
      PostEventHandlerCallback postEventHandlerCallback) {
    _postEventHandlerCallback = null;
  }

  public void onPause(Object eventInfo) {
    if (_preEventHandlerCallback != null) {
      _preEventHandlerCallback.onPause(eventInfo);
    }
    for (CloudEventListener listener : _eventListeners) {
      listener.onPause(eventInfo);
    }
    if (_postEventHandlerCallback != null) {
      _postEventHandlerCallback.onPause(eventInfo);
    }
  }

  public void onResume(Object eventInfo) {
    if (_preEventHandlerCallback != null) {
      _preEventHandlerCallback.onResume(eventInfo);
    }
    for (CloudEventListener listener : _eventListeners) {
      listener.onResume(eventInfo);
    }
    if (_postEventHandlerCallback != null) {
      _postEventHandlerCallback.onResume(eventInfo);
    }
  }
}
