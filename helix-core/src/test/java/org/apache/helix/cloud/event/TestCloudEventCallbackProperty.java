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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixCloudProperty;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperty;
import org.apache.helix.cloud.event.helix.CloudEventCallbackProperty;
import org.apache.helix.cloud.event.helix.CloudEventCallbackProperty.HelixOperation;
import org.apache.helix.cloud.event.helix.CloudEventCallbackProperty.UserDefinedCallbackType;
import org.apache.helix.cloud.event.helix.HelixCloudEventListener;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestCloudEventCallbackProperty {
  private HelixManager _helixManager;
  private HelixCloudProperty _cloudProperty;
  private final static String CLUSTER_NAME = "testCluster";

  @BeforeClass
  public void beforeClass() throws Exception {
    // Set up Helix manager property: Helix Cloud Property
    _cloudProperty = new HelixCloudProperty(new CloudConfig(new ZNRecord(CLUSTER_NAME)));
    _cloudProperty.setCloudEventCallbackEnabled(true);
    HelixManagerProperty.Builder managerPropertyBuilder = new HelixManagerProperty.Builder();
    managerPropertyBuilder.setHelixCloudProperty(_cloudProperty);

    // Build Helix manager property
    HelixManagerProperty managerProperty = managerPropertyBuilder.build();

    // Create Helix Manager
    _helixManager = new MockCloudEventAwareHelixManager(managerProperty);
  }

  @AfterTest
  public void afterTest() {
    _helixManager.disconnect();
    _cloudProperty.getCloudEventCallbackProperty()
        .setHelixOperationEnabled(HelixOperation.ENABLE_DISABLE_INSTANCE, false);
    _cloudProperty.getCloudEventCallbackProperty()
        .setHelixOperationEnabled(HelixOperation.MAINTENANCE_MODE, false);
    _cloudProperty.getCloudEventCallbackProperty()
        .unregisterUserDefinedCallback(UserDefinedCallbackType.PRE_ON_PAUSE);
    _cloudProperty.getCloudEventCallbackProperty()
        .unregisterUserDefinedCallback(UserDefinedCallbackType.POST_ON_PAUSE);
    _cloudProperty.getCloudEventCallbackProperty()
        .unregisterUserDefinedCallback(UserDefinedCallbackType.PRE_ON_RESUME);
    _cloudProperty.getCloudEventCallbackProperty()
        .unregisterUserDefinedCallback(UserDefinedCallbackType.POST_ON_RESUME);
    MockCloudEventCallbackImpl.triggeredOperation.clear();
  }

  @Test
  public void testOptionalHelixOperation() throws Exception {
    // Cloud event callback property
    Map<String, String> paramMap = new HashMap<>();
    paramMap.put(CloudEventCallbackProperty.UserArgsInputKey.CALLBACK_IMPL_CLASS_NAME,
        MockCloudEventCallbackImpl.class.getCanonicalName());
    paramMap.put(CloudEventCallbackProperty.UserArgsInputKey.CLOUD_EVENT_HANDLER_CLASS_NAME,
        HelixTestCloudEventHandler.class.getCanonicalName());
    CloudEventCallbackProperty property = new CloudEventCallbackProperty(paramMap);
    property.setHelixOperationEnabled(HelixOperation.ENABLE_DISABLE_INSTANCE, true);
    _cloudProperty.setCloudEventCallbackProperty(property);

    _helixManager.connect();

    // Manually trigger event
    ((CloudEventHandler)CloudEventHandlerFactory.getInstance(HelixTestCloudEventHandler.class.getCanonicalName()))
        .performAction(HelixCloudEventListener.EventType.ON_PAUSE, null);
    Assert.assertTrue(
        callbackTriggered(MockCloudEventCallbackImpl.OperationType.ON_PAUSE_DISABLE_INSTANCE));
    Assert.assertFalse(
        callbackTriggered(MockCloudEventCallbackImpl.OperationType.ON_PAUSE_MAINTENANCE_MODE));
    Assert.assertFalse(
        callbackTriggered(MockCloudEventCallbackImpl.OperationType.ON_RESUME_MAINTENANCE_MODE));
    Assert.assertFalse(
        callbackTriggered(MockCloudEventCallbackImpl.OperationType.ON_RESUME_ENABLE_INSTANCE));

    property.setHelixOperationEnabled(HelixOperation.MAINTENANCE_MODE, true);

    MockCloudEventCallbackImpl.triggeredOperation.clear();

    // Manually trigger event
    ((CloudEventHandler)CloudEventHandlerFactory.getInstance(HelixTestCloudEventHandler.class.getCanonicalName()))
        .performAction(HelixCloudEventListener.EventType.ON_PAUSE, null);
    Assert.assertTrue(
        callbackTriggered(MockCloudEventCallbackImpl.OperationType.ON_PAUSE_DISABLE_INSTANCE));
    Assert.assertTrue(
        callbackTriggered(MockCloudEventCallbackImpl.OperationType.ON_PAUSE_MAINTENANCE_MODE));
    Assert.assertFalse(
        callbackTriggered(MockCloudEventCallbackImpl.OperationType.ON_RESUME_MAINTENANCE_MODE));
    Assert.assertFalse(
        callbackTriggered(MockCloudEventCallbackImpl.OperationType.ON_RESUME_ENABLE_INSTANCE));

    MockCloudEventCallbackImpl.triggeredOperation.clear();

    // Manually trigger event
    ((CloudEventHandler) CloudEventHandlerFactory.getInstance(HelixTestCloudEventHandler.class.getCanonicalName()))
        .performAction(HelixCloudEventListener.EventType.ON_RESUME, null);
    Assert.assertFalse(
        callbackTriggered(MockCloudEventCallbackImpl.OperationType.ON_PAUSE_DISABLE_INSTANCE));
    Assert.assertFalse(
        callbackTriggered(MockCloudEventCallbackImpl.OperationType.ON_PAUSE_MAINTENANCE_MODE));
    Assert.assertTrue(
        callbackTriggered(MockCloudEventCallbackImpl.OperationType.ON_RESUME_ENABLE_INSTANCE));
    Assert.assertTrue(
        callbackTriggered(MockCloudEventCallbackImpl.OperationType.ON_RESUME_MAINTENANCE_MODE));
  }

  @Test
  public void testUserDefinedCallback() throws Exception {
    afterTest();
    // Cloud event callback property
    CloudEventCallbackProperty property = new CloudEventCallbackProperty(Collections
        .singletonMap(CloudEventCallbackProperty.UserArgsInputKey.CALLBACK_IMPL_CLASS_NAME,
            MockCloudEventCallbackImpl.class.getCanonicalName()));
    _cloudProperty.setCloudEventCallbackProperty(property);

    _helixManager.connect();

    property
        .registerUserDefinedCallback(UserDefinedCallbackType.PRE_ON_PAUSE, (manager, eventInfo) -> {
          MockCloudEventCallbackImpl.triggeredOperation
              .add(MockCloudEventCallbackImpl.OperationType.PRE_ON_PAUSE);
        });
    property.registerUserDefinedCallback(UserDefinedCallbackType.POST_ON_PAUSE,
        (manager, eventInfo) -> {
          MockCloudEventCallbackImpl.triggeredOperation
              .add(MockCloudEventCallbackImpl.OperationType.POST_ON_PAUSE);
        });
    property.registerUserDefinedCallback(UserDefinedCallbackType.PRE_ON_RESUME,
        (manager, eventInfo) -> {
          MockCloudEventCallbackImpl.triggeredOperation
              .add(MockCloudEventCallbackImpl.OperationType.PRE_ON_RESUME);
        });
    property.registerUserDefinedCallback(UserDefinedCallbackType.POST_ON_RESUME,
        (manager, eventInfo) -> {
          MockCloudEventCallbackImpl.triggeredOperation
              .add(MockCloudEventCallbackImpl.OperationType.POST_ON_RESUME);
        });

    // Manually trigger event
    ((CloudEventHandler) CloudEventHandlerFactory.getInstance(CloudEventHandler.class.getCanonicalName()))
        .performAction(HelixCloudEventListener.EventType.ON_PAUSE, null);
    Assert.assertTrue(callbackTriggered(MockCloudEventCallbackImpl.OperationType.PRE_ON_PAUSE));
    Assert.assertTrue(callbackTriggered(MockCloudEventCallbackImpl.OperationType.POST_ON_PAUSE));
    Assert.assertFalse(callbackTriggered(MockCloudEventCallbackImpl.OperationType.PRE_ON_RESUME));
    Assert.assertFalse(callbackTriggered(MockCloudEventCallbackImpl.OperationType.POST_ON_RESUME));

    MockCloudEventCallbackImpl.triggeredOperation.clear();

    ((CloudEventHandler) CloudEventHandlerFactory.getInstance(CloudEventHandler.class.getCanonicalName()))
        .performAction(HelixCloudEventListener.EventType.ON_RESUME, null);
    Assert.assertFalse(callbackTriggered(MockCloudEventCallbackImpl.OperationType.PRE_ON_PAUSE));
    Assert.assertFalse(callbackTriggered(MockCloudEventCallbackImpl.OperationType.POST_ON_PAUSE));
    Assert.assertTrue(callbackTriggered(MockCloudEventCallbackImpl.OperationType.PRE_ON_RESUME));
    Assert.assertTrue(callbackTriggered(MockCloudEventCallbackImpl.OperationType.POST_ON_RESUME));
  }

  @Test
  public void testUsingInvalidImplClassName() throws Exception {
    // Cloud event callback property
    CloudEventCallbackProperty property = new CloudEventCallbackProperty(Collections
        .singletonMap(CloudEventCallbackProperty.UserArgsInputKey.CALLBACK_IMPL_CLASS_NAME,
            "org.apache.helix.cloud.InvalidClassName"));
    _cloudProperty.setCloudEventCallbackProperty(property);

    _helixManager.connect();
  }

  @Test
  public void testRegisterAndUnregister() throws Exception {
    // Cloud event callback property
    Map<String, String> paramMap = new HashMap<>();
    paramMap.put(CloudEventCallbackProperty.UserArgsInputKey.CALLBACK_IMPL_CLASS_NAME,
        MockCloudEventCallbackImpl.class.getCanonicalName());
    paramMap.put(CloudEventCallbackProperty.UserArgsInputKey.CLOUD_EVENT_HANDLER_CLASS_NAME,
        HelixTestCloudEventHandler.class.getCanonicalName());
    CloudEventCallbackProperty property = new CloudEventCallbackProperty(paramMap);
    property.setHelixOperationEnabled(HelixOperation.ENABLE_DISABLE_INSTANCE, true);
    _cloudProperty.setCloudEventCallbackProperty(property);

    _helixManager.connect();

    Assert.assertTrue(HelixTestCloudEventHandler.anyListenerIsRegisterFlag);

    _helixManager.disconnect();
    Assert.assertFalse(HelixTestCloudEventHandler.anyListenerIsRegisterFlag);
  }

  @Test
  public void testUsingInvalidHandlerClassName() throws Exception {
    // Cloud event callback property
    CloudEventCallbackProperty property = new CloudEventCallbackProperty(Collections
        .singletonMap(CloudEventCallbackProperty.UserArgsInputKey.CLOUD_EVENT_HANDLER_CLASS_NAME,
            "org.apache.helix.cloud.InvalidClassName"));
    _cloudProperty.setCloudEventCallbackProperty(property);

    try {
    _helixManager.connect();}
    catch (Exception ex){
      Assert.assertEquals(ex.getClass(), java.lang.ClassNotFoundException.class);
    }

    // Manually trigger event
    ((CloudEventHandler) CloudEventHandlerFactory.getInstance(CloudEventHandler.class.getCanonicalName()))
        .performAction(HelixCloudEventListener.EventType.ON_PAUSE, null);
  }

  private boolean callbackTriggered(MockCloudEventCallbackImpl.OperationType type) {
    return MockCloudEventCallbackImpl.triggeredOperation.contains(type);
  }
}
