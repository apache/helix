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

import java.util.List;

import org.apache.helix.HelixManager;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.util.InstanceValidationUtil;

/**
 * A default callback implementation class to be used in {@link HelixCloudEventListener}
 */
public class DefaultCloudEventCallbackImpl {
  public static final String REASON = "CloudEvent";

  /**
   * Disable the instance
   * @param manager The helix manager associated with the listener
   * @param eventInfo Detailed information about the event
   */
  public void disableInstance(HelixManager manager, Object eventInfo) {
    manager.getClusterManagmentTool()
        .enableInstance(manager.getClusterName(), manager.getInstanceName(), false,
            InstanceConstants.InstanceDisabledType.CLOUD_EVENT, null);
  }

  /**
   * Enable the instance
   * @param manager The helix manager associated with the listener
   * @param eventInfo Detailed information about the event
   */
  public void enableInstance(HelixManager manager, Object eventInfo) {
    manager.getClusterManagmentTool()
        .enableInstance(manager.getClusterName(), manager.getInstanceName(), true);
  }

  /**
   * Put cluster into maintenance mode if the cluster is not currently in maintenance mode
   * @param manager The helix manager associated with the listener
   * @param eventInfo Detailed information about the event
   */
  public void enterMaintenanceMode(HelixManager manager, Object eventInfo) {
    if (!manager.getClusterManagmentTool().isInMaintenanceMode(manager.getClusterName())) {
      manager.getClusterManagmentTool()
          .manuallyEnableMaintenanceMode(manager.getClusterName(), true, REASON, null);
    }
  }

  /**
   * Exit maintenance mode for the cluster, if there is no more live instances disabled for cloud event
   * @param manager The helix manager associated with the listener
   * @param eventInfo Detailed information about the event
   */
  public void exitMaintenanceMode(HelixManager manager, Object eventInfo) {
    List<String> instances =
        manager.getClusterManagmentTool().getInstancesInCluster(manager.getClusterName());
    // Check if there is any disabled live instance that was disabled due to cloud event,
    // if none left, exit maintenance mode
    if (instances.stream().noneMatch(
        instance -> !InstanceValidationUtil.isEnabled(manager.getHelixDataAccessor(), instance)
            && manager.getConfigAccessor().getInstanceConfig(manager.getClusterName(), instance)
            .getInstanceDisabledType()
            .equals(InstanceConstants.InstanceDisabledType.CLOUD_EVENT.name())
            && InstanceValidationUtil.isAlive(manager.getHelixDataAccessor(), instance))) {
      manager.getClusterManagmentTool()
          .manuallyEnableMaintenanceMode(manager.getClusterName(), false, REASON, null);
    }
  }
}
