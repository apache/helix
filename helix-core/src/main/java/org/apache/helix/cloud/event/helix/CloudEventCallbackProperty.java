package org.apache.helix.cloud.event.helix;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A property for users to customize the behavior of a Helix manager as a cloud event listener
 */
public class CloudEventCallbackProperty {
  private static final Logger LOG =
      LoggerFactory.getLogger(CloudEventCallbackProperty.class.getName());

  private Set<HelixOperation> _enabledHelixOperation;
  private Map<UserDefinedCallbackType, BiConsumer<HelixManager, Object>> _userDefinedCallbackMap;
  private final Map<String, String> _userArgs;

  /**
   * Constructor
   * @param userArgs A map contains information that users pass in
   */
  public CloudEventCallbackProperty(Map<String, String> userArgs) {
    _enabledHelixOperation = new HashSet<>();
    _userDefinedCallbackMap = new HashMap<>();
    _userArgs = userArgs;
  }

  /**
   * Keys for retrieve information from the map users pass in
   */
  public static class UserArgsInputKey {
    public static final String CALLBACK_IMPL_CLASS_NAME = "callbackImplClassName";
    public static final String CLOUD_EVENT_HANDLER_CLASS_NAME = "cloudEventHandlerClassName";
  }

  /**
   * A collection of types of Helix operations
   */
  public enum HelixOperation {
    ENABLE_DISABLE_INSTANCE,
    MAINTENANCE_MODE
  }

  /**
   * A collection of types and positions for user to plug in customized callback
   */
  public enum UserDefinedCallbackType {
    PRE_ON_PAUSE,
    POST_ON_PAUSE,
    PRE_ON_RESUME,
    POST_ON_RESUME,
  }

  /**
   * Enable an Helix-supported operation
   * The operation is implemented in the callback impl class
   * @param operation operation type
   */
  public void setHelixOperationEnabled(HelixOperation operation, boolean enabled) {
    if (enabled) {
      _enabledHelixOperation.add(operation);
    } else {
      _enabledHelixOperation.remove(operation);
    }
  }

  /**
   * Register a user defined callback at a user specified position
   * The position is relative to Helix operations
   * There are two options for each type (onPause or onResume):
   * 1. PRE: The user defined callback will be the first callback being called in the listener
   * 2. POST: The user defined callback will be the last callback being called in the listener
   * @param callbackType The type and position for registering the callback
   * @param callback The implementation of the callback
   */
  public void registerUserDefinedCallback(UserDefinedCallbackType callbackType,
      BiConsumer<HelixManager, Object> callback) {
    LOG.info("Registering callback {} as {} type user defined callback...", callback,
        callbackType.name());
    _userDefinedCallbackMap.put(callbackType, callback);
  }

  /**
   * Unregister a user defined callback at a user specified position
   * @param callbackType The type and position for registering the callback
   */
  public void unregisterUserDefinedCallback(UserDefinedCallbackType callbackType) {
    LOG.info("Unregistering {} type user defined callback...", callbackType.name());
    _userDefinedCallbackMap.remove(callbackType);
  }

  /**
   * Get the user passed-in information
   * @return Empty map if not defined; an unmodified map otherwise
   */
  public Map<String, String> getUserArgs() {
    return _userArgs == null ? Collections.emptyMap() : Collections.unmodifiableMap(_userArgs);
  }

  /**
   * Get the map where user defined callbacks are stored
   */
  public Map<UserDefinedCallbackType, BiConsumer<HelixManager, Object>> getUserDefinedCallbackMap() {
    return _userDefinedCallbackMap;
  }

  /**
   * Get the set where enabled Helix operations are stored
   */
  public Set<HelixOperation> getEnabledHelixOperation() {
    return _enabledHelixOperation;
  }
}
