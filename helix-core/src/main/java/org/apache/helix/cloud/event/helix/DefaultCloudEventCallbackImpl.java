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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.util.InstanceValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A default callback implementation class to be used in {@link HelixCloudEventListener}
 */
public class DefaultCloudEventCallbackImpl {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultCloudEventCallbackImpl.class);
  private final String _instanceReason = "Cloud event in DefaultCloudEventCallback at %s";
  private final String _emmReason = "Cloud event EMM in DefaultCloudEventCallback by %s at %s";

  /**
   * Disable the instance and track the cloud event in map field disabledInstancesWithInfo in
   * cluster config. Will not re-disable the instance if the instance is already disabled for
   * other reason. (So we will not overwrite the disabled reason and enable this instance when
   * on-unpause)
   * @param manager The helix manager associated with the listener
   * @param eventInfo Detailed information about the event
   */
  public void disableInstance(HelixManager manager, Object eventInfo) {
    String message = String.format(_instanceReason, System.currentTimeMillis());
    LOG.info("DefaultCloudEventCallbackImpl disable Instance {}", manager.getInstanceName());
    if (InstanceValidationUtil
        .isEnabled(manager.getHelixDataAccessor(), manager.getInstanceName())) {
      manager.getClusterManagmentTool()
          .enableInstance(manager.getClusterName(), manager.getInstanceName(), false,
              InstanceConstants.InstanceDisabledType.CLOUD_EVENT, message);
    }
    HelixEventHandlingUtil.updateCloudEventOperationInClusterConfig(manager.getClusterName(),
        manager.getInstanceName(), manager.getHelixDataAccessor().getBaseDataAccessor(), false,
        message);
  }

  /**
   * Remove tracked cloud event in cluster config and enable the instance
   * We only enable instance that is disabled because of cloud event.
   * @param manager The helix manager associated with the listener
   * @param eventInfo Detailed information about the event
   */
  public void enableInstance(HelixManager manager, Object eventInfo) {
    LOG.info("DefaultCloudEventCallbackImpl enable Instance {}", manager.getInstanceName());
    String instanceName = manager.getInstanceName();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    String message = String.format(_instanceReason, System.currentTimeMillis());
    HelixEventHandlingUtil
        .updateCloudEventOperationInClusterConfig(manager.getClusterName(), instanceName,
            manager.getHelixDataAccessor().getBaseDataAccessor(), true, message);
    if (HelixEventHandlingUtil.isInstanceDisabledForCloudEvent(instanceName, accessor)) {
      manager.getClusterManagmentTool().enableInstance(manager.getClusterName(), instanceName, true,
          InstanceConstants.InstanceDisabledType.CLOUD_EVENT, message);
    }
  }

  /**
   * Will enter MM when the cluster is not in MM
   * TODO: we should add maintenance reason when EMM with cloud event
   * @param manager The helix manager associated with the listener
   * @param eventInfo Detailed information about the event
   */
  public void enterMaintenanceMode(HelixManager manager, Object eventInfo) {
      if (!manager.getClusterManagmentTool().isInMaintenanceMode(manager.getClusterName())) {
        LOG.info("DefaultCloudEventCallbackImpl enterMaintenanceMode by {}",
            manager.getInstanceName());
        manager.getClusterManagmentTool()
            .manuallyEnableMaintenanceMode(manager.getClusterName(), true,
                String.format(_emmReason, manager.getInstanceName(), System.currentTimeMillis()),
                null);
      }
  }

  /**
   * Will exit MM when when cluster config tracks no ongoing cloud event being handling
   * TODO: we should also check the maintenance reason and only exit when EMM is caused by cloud event
   * @param manager The helix manager associated with the listener
   * @param eventInfo Detailed information about the event
   */
  public void exitMaintenanceMode(HelixManager manager, Object eventInfo) {
    ClusterConfig clusterConfig = manager.getHelixDataAccessor()
        .getProperty(manager.getHelixDataAccessor().keyBuilder().clusterConfig());
    if (HelixEventHandlingUtil.checkNoInstanceUnderCloudEvent(clusterConfig)) {
      LOG.info("DefaultCloudEventCallbackImpl exitMaintenanceMode by {}",
          manager.getInstanceName());
      manager.getClusterManagmentTool()
          .manuallyEnableMaintenanceMode(manager.getClusterName(), false,
              String.format(_emmReason, manager.getInstanceName(), System.currentTimeMillis()),
              null);
    } else {
      LOG.info(
          "DefaultCloudEventCallbackImpl will not exitMaintenanceMode as there are {} instances under cloud event",
          clusterConfig.getDisabledInstancesWithInfo().keySet().size());
    }
  }
}
