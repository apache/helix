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

import org.apache.helix.HelixCloudProperty;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixManagerProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.cloud.event.helix.CloudEventCallbackProperty;
import org.apache.helix.cloud.event.helix.CloudEventCallbackProperty.OptionalHelixOperation;
import org.apache.helix.cloud.event.helix.CloudEventCallbackProperty.UserDefinedCallbackType;
import org.apache.helix.manager.zk.HelixManagerStateListener;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCloudEventCallbackProperty {

  @Test
  public void testOptionalHelixOperation() {
    // Cloud event callback property
    CloudEventCallbackProperty property = new CloudEventCallbackProperty(Collections
        .singletonMap(CloudEventCallbackProperty.CALLBACK_IMPL_CLASS_NAME,
            MockCloudEventCallbackImpl.class.getCanonicalName()));
    property.enableOptionalHelixOperation(OptionalHelixOperation.MAINTENANCE_MODE);

    // Cloud property
    HelixCloudProperty cloudProperty =
        new HelixCloudProperty(new CloudConfig(new ZNRecord("test")));
    cloudProperty.setCloudEventCallbackEnabled(true);
    cloudProperty.setCloudEventCallbackProperty(property);

    // Helix manager property
    HelixManagerProperty.Builder managerPropertyBuilder = new HelixManagerProperty.Builder();
    managerPropertyBuilder.setHelixCloudProperty(cloudProperty);
    HelixManagerProperty managerProperty = managerPropertyBuilder.build();
    HelixManager helixManager = HelixManagerFactory
        .getZKHelixManager("clusterName", "instanceName", InstanceType.PARTICIPANT,
            new HelixManagerStateListener() {
              @Override
              public void onConnected(HelixManager helixManager) throws Exception {

              }

              @Override
              public void onDisconnected(HelixManager helixManager, Throwable error)
                  throws Exception {

              }
            }, managerProperty);

    CloudEventHandlerFactory.getInstance().onPause(null);
    Assert.assertTrue(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.ON_PAUSE_DEFAULT_HELIX));
    Assert.assertTrue(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.ON_PAUSE_MAINTENANCE_MODE));
    Assert.assertFalse(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.ON_RESUME_DEFAULT_HELIX));
    Assert.assertFalse(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.ON_RESUME_MAINTENANCE_MODE));

    MockCloudEventCallbackImpl.triggeredOperation.clear();

    CloudEventHandlerFactory.getInstance().onResume(null);
    Assert.assertFalse(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.ON_PAUSE_DEFAULT_HELIX));
    Assert.assertFalse(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.ON_PAUSE_MAINTENANCE_MODE));
    Assert.assertTrue(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.ON_RESUME_DEFAULT_HELIX));
    Assert.assertTrue(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.ON_RESUME_MAINTENANCE_MODE));
  }

  @Test
  public void testUserDefinedCallback() {
    // Cloud event callback property
    CloudEventCallbackProperty property = new CloudEventCallbackProperty(Collections
        .singletonMap(CloudEventCallbackProperty.CALLBACK_IMPL_CLASS_NAME,
            MockCloudEventCallbackImpl.class.getCanonicalName()));

    // Cloud property
    HelixCloudProperty cloudProperty =
        new HelixCloudProperty(new CloudConfig(new ZNRecord("test")));
    cloudProperty.setCloudEventCallbackEnabled(true);
    cloudProperty.setCloudEventCallbackProperty(property);

    // Helix manager property
    HelixManagerProperty.Builder managerPropertyBuilder = new HelixManagerProperty.Builder();
    managerPropertyBuilder.setHelixCloudProperty(cloudProperty);
    HelixManagerProperty managerProperty = managerPropertyBuilder.build();
    HelixManager helixManager = HelixManagerFactory
        .getZKHelixManager("clusterName", "instanceName", InstanceType.PARTICIPANT,
            new HelixManagerStateListener() {
              @Override
              public void onConnected(HelixManager helixManager) throws Exception {

              }

              @Override
              public void onDisconnected(HelixManager helixManager, Throwable error)
                  throws Exception {

              }
            }, managerProperty);

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

    CloudEventHandlerFactory.getInstance().onPause(null);
    Assert.assertTrue(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.PRE_ON_PAUSE));
    Assert.assertTrue(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.POST_ON_PAUSE));
    Assert.assertFalse(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.PRE_ON_RESUME));
    Assert.assertFalse(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.POST_ON_RESUME));

    MockCloudEventCallbackImpl.triggeredOperation.clear();

    CloudEventHandlerFactory.getInstance().onResume(null);
    Assert.assertFalse(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.PRE_ON_PAUSE));
    Assert.assertFalse(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.POST_ON_PAUSE));
    Assert.assertTrue(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.PRE_ON_RESUME));
    Assert.assertTrue(MockCloudEventCallbackImpl.triggeredOperation
        .contains(MockCloudEventCallbackImpl.OperationType.POST_ON_RESUME));
  }
}
