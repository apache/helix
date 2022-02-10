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

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudEventCallbackProperty {
  private static final Logger LOG =
      LoggerFactory.getLogger(CloudEventCallbackProperty.class.getName());

  private ConcurrentHashMap<OnPauseOperations, BiConsumer<HelixManager, Object>>
      _onPauseOperationMap;
  private ConcurrentHashMap<OnResumeOperations, BiConsumer<HelixManager, Object>>
      _onResumeOperationMap;
  public static final String CLOUD_EVENT_CALLBACK_IMPL_CLASS_NAME =
      "helix.cloud.event.cloudEventCallbackImplClassName";
  private AbstractCloudEventCallbackImpl _callbackImplClass;

  private CloudEventCallbackProperty() {
    String cloudEventCallbackImplClassName =
        System.getProperty(CLOUD_EVENT_CALLBACK_IMPL_CLASS_NAME);
    if (cloudEventCallbackImplClassName == null || cloudEventCallbackImplClassName.isEmpty()) {
      String errMsg = "No cloud event callback implementation class name is defined.";
      LOG.error(errMsg);
      throw new HelixException(errMsg);
    }

    _callbackImplClass = loadCloudEventCallbackOperations(cloudEventCallbackImplClassName);

    if (_callbackImplClass == null) {
      throw new HelixException("No cloud event callback implementation class name is found.");
    }

    _onPauseOperationMap = new ConcurrentHashMap<>();
    _onResumeOperationMap = new ConcurrentHashMap<>();
  }

  public enum OptionalHelixOperation {
    MAINTENANCE_MODE
  }

  public enum UserDefinedCallbackType {
    PRE_ON_PAUSE, POST_ON_PAUSE, PRE_ON_RESUME, POST_ON_RESUME,
  }

  private enum OnPauseOperations {
    PRE_ON_PAUSE, MAINTENANCE_MODE, DEFAULT_HELIX_OPERATION, POST_ON_PAUSE
  }

  private enum OnResumeOperations {
    PRE_ON_RESUME, DEFAULT_HELIX_OPERATION, MAINTENANCE_MODE, POST_ON_RESUME,
  }

  public void onPause(HelixManager helixManager, Object eventInfo) {
    Iterator<OnPauseOperations> iterator = Arrays.stream(OnPauseOperations.values()).iterator();
    while (iterator.hasNext()) {
      _onPauseOperationMap.getOrDefault(iterator.next(), (manager, info) -> {
      }).accept(helixManager, eventInfo);
    }
  }

  public void onResume(HelixManager helixManager, Object eventInfo) {
    Iterator<OnResumeOperations> iterator = Arrays.stream(OnResumeOperations.values()).iterator();
    while (iterator.hasNext()) {
      _onResumeOperationMap.getOrDefault(iterator.next(), (manager, info) -> {
      }).accept(helixManager, eventInfo);
    }
  }

  public void enableOptionalHelixOperation(OptionalHelixOperation operation) {
    if (operation == OptionalHelixOperation.MAINTENANCE_MODE) {
      _onPauseOperationMap
          .put(OnPauseOperations.MAINTENANCE_MODE, _callbackImplClass::onPauseMaintenanceMode);
      _onResumeOperationMap
          .put(OnResumeOperations.MAINTENANCE_MODE, _callbackImplClass::onResumeMaintenanceMode);
    }
  }

  public void disableOptionalHelixOperation(OptionalHelixOperation operation) {
    if (operation == OptionalHelixOperation.MAINTENANCE_MODE) {
      _onPauseOperationMap.remove(OnPauseOperations.MAINTENANCE_MODE);
      _onResumeOperationMap.remove(OnResumeOperations.MAINTENANCE_MODE);
    }
  }

  public void registerUserDefinedCallback(UserDefinedCallbackType callbackType,
      BiConsumer<HelixManager, Object> callback) {
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

  public void unregisterUserDefinedCallback(UserDefinedCallbackType callbackType) {
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

  private AbstractCloudEventCallbackImpl loadCloudEventCallbackOperations(String implClassName) {
    AbstractCloudEventCallbackImpl implClass;
    try {
      LOG.info("Loading class: " + implClassName);
      implClass = (AbstractCloudEventCallbackImpl) HelixUtil.loadClass(getClass(), implClassName)
          .newInstance();
    } catch (Exception e) {
      String errMsg = String
          .format("No cloud event callback implementation class found for: %s. message: ",
              implClassName);
      LOG.error(errMsg, e);
      throw new HelixException(String.format(errMsg, e));
    }
    return implClass;
  }
}
