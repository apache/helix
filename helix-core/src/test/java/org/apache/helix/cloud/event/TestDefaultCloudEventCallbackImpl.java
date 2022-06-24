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

import org.apache.helix.HelixAdmin;
import org.apache.helix.cloud.event.helix.DefaultCloudEventCallbackImpl;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.util.InstanceValidationUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestDefaultCloudEventCallbackImpl extends ZkStandAloneCMTestBase {
  private final DefaultCloudEventCallbackImpl _impl = new DefaultCloudEventCallbackImpl();
  private MockParticipantManager _instanceManager;
  private HelixAdmin _admin;

  public TestDefaultCloudEventCallbackImpl() throws IllegalAccessException, InstantiationException {
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _instanceManager = _participants[0];
    _admin = _instanceManager.getClusterManagmentTool();
  }

  @Test
  public void testDisableInstance() {
    Assert.assertTrue(InstanceValidationUtil
        .isEnabled(_manager.getHelixDataAccessor(), _instanceManager.getInstanceName()));
    _impl.disableInstance(_instanceManager, null);
    Assert.assertFalse(InstanceValidationUtil
        .isEnabled(_manager.getHelixDataAccessor(), _instanceManager.getInstanceName()));
    Assert.assertEquals(_manager.getConfigAccessor()
        .getInstanceConfig(CLUSTER_NAME, _instanceManager.getInstanceName())
        .getInstanceDisabledType(), InstanceConstants.InstanceDisabledType.CLOUD_EVENT.name());

    // Should not disable instance if it is already disabled due to other reasons
    // And disabled type should remain unchanged
    _admin.enableInstance(CLUSTER_NAME, _instanceManager.getInstanceName(), false);
    _impl.disableInstance(_instanceManager, null);
    Assert.assertFalse(InstanceValidationUtil
        .isEnabled(_manager.getHelixDataAccessor(), _instanceManager.getInstanceName()));
    Assert.assertEquals(_manager.getConfigAccessor()
            .getInstanceConfig(CLUSTER_NAME, _instanceManager.getInstanceName())
            .getInstanceDisabledType(),
        InstanceConstants.InstanceDisabledType.DEFAULT_INSTANCE_DISABLE_TYPE.name());

    _admin.enableInstance(CLUSTER_NAME, _instanceManager.getInstanceName(), false,
        InstanceConstants.InstanceDisabledType.CLOUD_EVENT, null);
  }

  @Test (dependsOnMethods = "testDisableInstance")
  public void testEnableInstance() {
    Assert.assertFalse(InstanceValidationUtil
        .isEnabled(_manager.getHelixDataAccessor(), _instanceManager.getInstanceName()));
    // Should enable instance if the instance is disabled due to cloud event
    _impl.enableInstance(_instanceManager, null);
    Assert.assertTrue(InstanceValidationUtil
        .isEnabled(_manager.getHelixDataAccessor(), _instanceManager.getInstanceName()));

    // Should not enable instance if it is not disabled due to cloud event
    _admin.enableInstance(CLUSTER_NAME, _instanceManager.getInstanceName(), false);
    _impl.enableInstance(_instanceManager, null);
    Assert.assertFalse(InstanceValidationUtil
        .isEnabled(_manager.getHelixDataAccessor(), _instanceManager.getInstanceName()));
    _admin.enableInstance(_instanceManager.getClusterName(), _instanceManager.getInstanceName(),
        true);
  }

  @Test
  public void testEnterMaintenanceMode() {
    Assert.assertFalse(_admin.isInMaintenanceMode(CLUSTER_NAME));
    _impl.enterMaintenanceMode(_instanceManager, null);
    _impl.disableInstance(_instanceManager, null);
    Assert.assertTrue(_admin.isInMaintenanceMode(CLUSTER_NAME));
  }

  @Test (dependsOnMethods = "testEnterMaintenanceMode")
  public void testExitMaintenanceMode() {
    Assert.assertTrue(_admin.isInMaintenanceMode(CLUSTER_NAME));
    // Should not exit maintenance mode if there is remaining live instance that is disabled due to cloud event
    _impl.exitMaintenanceMode(_instanceManager, null);
    Assert.assertTrue(_admin.isInMaintenanceMode(CLUSTER_NAME));

    // Should exit maintenance mode if there is no remaining live instance that is disabled due to cloud event
    _impl.enableInstance(_instanceManager, null);
    _impl.exitMaintenanceMode(_instanceManager, null);
    Assert.assertFalse(_admin.isInMaintenanceMode(CLUSTER_NAME));
  }
}