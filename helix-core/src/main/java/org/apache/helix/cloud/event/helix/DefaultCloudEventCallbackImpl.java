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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.util.InstanceValidationUtil;

/**
 * A default callback implementation class to be used in {@link HelixCloudEventListener}
 */
public class DefaultCloudEventCallbackImpl {
  private final String _reason =
      "Cloud event callback %s in class %s triggered in listener HelixManager %s, at time %s .";
  protected final String _className = this.getClass().getSimpleName();

  /**
   * Disable the instance
   * @param manager The helix manager associated with the listener
   * @param eventInfo Detailed information about the event
   */
  public void disableInstance(HelixManager manager, Object eventInfo) {
    if (InstanceValidationUtil
        .isEnabled(manager.getHelixDataAccessor(), manager.getInstanceName())) {
      manager.getClusterManagmentTool()
          .enableInstance(manager.getClusterName(), manager.getInstanceName(), false,
              InstanceConstants.InstanceDisabledType.CLOUD_EVENT, String
                  .format(_reason, "disableInstance", _className, manager,
                      System.currentTimeMillis()));
    }
  }

  /**
   * Enable the instance
   * @param manager The helix manager associated with the listener
   * @param eventInfo Detailed information about the event
   */
  public void enableInstance(HelixManager manager, Object eventInfo) {
    String instanceName = manager.getInstanceName();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    if (!InstanceValidationUtil.isEnabled(accessor, instanceName) && InstanceValidationUtil
        .getInstanceHelixDisabledType(accessor, instanceName)
        .equals(InstanceConstants.InstanceDisabledType.CLOUD_EVENT.name())) {
      manager.getClusterManagmentTool()
          .enableInstance(manager.getClusterName(), instanceName, true);
    }
  }

  /**
   * Put cluster into maintenance mode if the cluster is not currently in maintenance mode
   * @param manager The helix manager associated with the listener
   * @param eventInfo Detailed information about the event
   */
  public void enterMaintenanceMode(HelixManager manager, Object eventInfo) {
    if (!manager.getClusterManagmentTool().isInMaintenanceMode(manager.getClusterName())) {
      manager.getClusterManagmentTool()
          .manuallyEnableMaintenanceMode(manager.getClusterName(), true, String
              .format(_reason, "enterMaintenanceMode", _className, manager,
                  System.currentTimeMillis()), null);
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
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    if (instances.stream().noneMatch(
        instance -> !InstanceValidationUtil.isEnabled(accessor, instance) && InstanceValidationUtil
            .getInstanceHelixDisabledType(accessor, instance)
            .equals(InstanceConstants.InstanceDisabledType.CLOUD_EVENT.name())
            && InstanceValidationUtil.isAlive(accessor, instance))) {
      manager.getClusterManagmentTool()
          .manuallyEnableMaintenanceMode(manager.getClusterName(), false, String
              .format(_reason, "exitMaintenanceMode", _className, manager,
                  System.currentTimeMillis()), null);
    }
  }
}
