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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A property for users to customize the behavior of a Helix manager as a cloud event listener
 * Use this
 */
public class CloudEventCallbackProperty {
  private static final Logger LOG =
      LoggerFactory.getLogger(CloudEventCallbackProperty.class.getName());

  // The key for user to pass in callback impl class name in the constructor
  public static final String CALLBACK_IMPL_CLASS_NAME = "callbackImplClassName";
  private Map<OnPauseOperations, BiConsumer<HelixManager, Object>>
      _onPauseOperationMap;
  private Map<OnResumeOperations, BiConsumer<HelixManager, Object>>
      _onResumeOperationMap;
  private final String _callbackImplClassName;

  /**
   * Constructor
   * @param userArgs A map contains information that users pass in
   */
  public CloudEventCallbackProperty(Map<String, String> userArgs) {
    _callbackImplClassName = userArgs.get(CALLBACK_IMPL_CLASS_NAME);
    _onPauseOperationMap = new ConcurrentHashMap<>();
    _onResumeOperationMap = new ConcurrentHashMap<>();
  }

  /**
   * A collection of types of optional Helix-supported operations
   */
  public enum OptionalHelixOperation {
    MAINTENANCE_MODE
  }

  /**
   * A collection of types and positions for user to plugin customized callback
   */
  public enum UserDefinedCallbackType {
    PRE_ON_PAUSE, POST_ON_PAUSE, PRE_ON_RESUME, POST_ON_RESUME,
  }

  private enum OnPauseOperations {
    PRE_ON_PAUSE, MAINTENANCE_MODE, DEFAULT_HELIX_OPERATION, POST_ON_PAUSE
  }

  private enum OnResumeOperations {
    PRE_ON_RESUME, DEFAULT_HELIX_OPERATION, MAINTENANCE_MODE, POST_ON_RESUME,
  }

  /**
   * Trigger the registered onPause callbacks one by one
   * Order:
   * 1. PRE onPause
   * 2. Helix optional: Maintenance Mode
   * 3. Helix default
   * 4. POST onPause
   * @param helixManager Helix manager to perform operations
   * @param eventInfo Information of the event provided by upsteam
   * @param callbackImplClass Implementation class for Helix default and optional operations
   */
  public void onPause(HelixManager helixManager, Object eventInfo,
      AbstractCloudEventCallbackImpl callbackImplClass) {
    for (OnPauseOperations operation : OnPauseOperations.values()) {
      switch (operation) {
        case DEFAULT_HELIX_OPERATION:
          callbackImplClass.onPauseDefaultHelixOperation(helixManager, eventInfo);
          break;
        case MAINTENANCE_MODE:
          callbackImplClass.onPauseMaintenanceMode(helixManager, eventInfo);
          break;
        default:
          _onPauseOperationMap.getOrDefault(operation, (manager, info) -> {
          }).accept(helixManager, eventInfo);
      }
    }
  }

  /**
   * Trigger the registered onResume callbacks one by one
   * Order:
   * 1. PRE onResume
   * 2. Helix default
   * 3. Helix optional: Maintenance Mode
   * 4. POST onResume
   * @param helixManager Helix manager to perform operations
   * @param eventInfo Information of the event provided by upsteam
   * @param callbackImplClass Implementation class for Helix default and optional operations
   */
  public void onResume(HelixManager helixManager, Object eventInfo,
      AbstractCloudEventCallbackImpl callbackImplClass) {
    for (OnResumeOperations operation : OnResumeOperations.values()) {
      switch (operation) {
        case DEFAULT_HELIX_OPERATION:
          callbackImplClass.onResumeDefaultHelixOperation(helixManager, eventInfo);
          break;
        case MAINTENANCE_MODE:
          callbackImplClass.onResumeMaintenanceMode(helixManager, eventInfo);
          break;
        default:
          _onResumeOperationMap.getOrDefault(operation, (manager, info) -> {
          }).accept(helixManager, eventInfo);
      }
    }
  }

  /**
   * Enable an Helix-supported optional operation
   * The operation is implemented in the callback impl class loaded in Helix manager
   * @param operation operation type
   */
  public void enableOptionalHelixOperation(OptionalHelixOperation operation) {
    LOG.info("Enabling {} type optional Helix operation...", operation.name());
    if (operation == OptionalHelixOperation.MAINTENANCE_MODE) {
      // Put a place holder first, it will be replaced with implementations from the loaded
      // callback impl class at call time
      _onPauseOperationMap.put(OnPauseOperations.MAINTENANCE_MODE, (manager, info) -> {
      });
      _onResumeOperationMap.put(OnResumeOperations.MAINTENANCE_MODE, (manager, info) -> {
      });
    }
  }

  /**
   * Disable an Helix-supported optional operation
   * @param operation operation type
   */
  public void disableOptionalHelixOperation(OptionalHelixOperation operation) {
    LOG.info("Disabling {} type optional Helix operation...", operation.name());
    if (operation == OptionalHelixOperation.MAINTENANCE_MODE) {
      _onPauseOperationMap.remove(OnPauseOperations.MAINTENANCE_MODE);
      _onResumeOperationMap.remove(OnResumeOperations.MAINTENANCE_MODE);
    }
  }

  /**
   * Register a user defined callback at a user specified position
   * The position is relative to Helix-supported logic (Helix default operation + Helix optional operation)
   * There are two options for each type (onPause or onResume):
   * 1. PRE: The user defined callback will be called before Helix-supported logic
   * 2. POST: The user defined callback will be called after Helix-supported logic finishes
   * @param callbackType The type and position for registering the callback
   * @param callback The actual implementation of the callback
   */
  public void registerUserDefinedCallback(UserDefinedCallbackType callbackType,
      BiConsumer<HelixManager, Object> callback) {
    LOG.info("Registering callback {} as {} type user defined callback...", callback,
        callbackType.name());
    switch (callbackType) {
      case PRE_ON_PAUSE:
        _onPauseOperationMap.put(OnPauseOperations.PRE_ON_PAUSE, callback);
        break;
      case POST_ON_PAUSE:
        _onPauseOperationMap.put(OnPauseOperations.POST_ON_PAUSE, callback);
        break;
      case PRE_ON_RESUME:
        _onResumeOperationMap.put(OnResumeOperations.PRE_ON_RESUME, callback);
        break;
      case POST_ON_RESUME:
        _onResumeOperationMap.put(OnResumeOperations.POST_ON_RESUME, callback);
        break;
      default:
        break;
    }
  }

  /**
   * Unregister a user defined callback at a user specified position
   * @param callbackType The type and position for registering the callback
   */
  public void unregisterUserDefinedCallback(UserDefinedCallbackType callbackType) {
    LOG.info("Unregistering {} type user defined callback...", callbackType.name());
    switch (callbackType) {
      case PRE_ON_PAUSE:
        _onPauseOperationMap.remove(OnPauseOperations.PRE_ON_PAUSE);
        break;
      case POST_ON_PAUSE:
        _onPauseOperationMap.remove(OnPauseOperations.POST_ON_PAUSE);
        break;
      case PRE_ON_RESUME:
        _onResumeOperationMap.remove(OnResumeOperations.PRE_ON_RESUME);
        break;
      case POST_ON_RESUME:
        _onResumeOperationMap.remove(OnResumeOperations.POST_ON_RESUME);
        break;
      default:
        break;
    }
  }

  /**
   * Get the user specified class name for the implementation class of Helix-supported callbacks
   * @return The user defined class name if not null;
   *         if null, return AbstractCloudEventCallbackImpl class name
   */
  public String getCallbackImplClassName() {
    return _callbackImplClassName == null ? AbstractCloudEventCallbackImpl.class.getCanonicalName()
        : _callbackImplClassName;
  }
}
