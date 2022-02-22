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
import org.apache.helix.HelixManager;

public abstract class AbstractCloudEventCallbackImpl {

  public void onPauseDefaultHelixOperation(HelixManager manager, Object eventInfo) {
    manager.getClusterManagmentTool()
        .enableInstance(manager.getClusterName(), manager.getInstanceName(), false);
  }

  public void onResumeDefaultHelixOperation(HelixManager manager, Object eventInfo) {
    manager.getClusterManagmentTool()
        .enableInstance(manager.getClusterName(), manager.getInstanceName(), true);
  }

  public void onPauseMaintenanceMode(HelixManager manager, Object eventInfo) {
    HelixAdmin admin = manager.getClusterManagmentTool();
    if (!admin.isInMaintenanceMode(manager.getClusterName())) {
      admin.manuallyEnableMaintenanceMode(manager.getClusterName(), true,
          "Cloud event is incoming: " + eventInfo.toString(), null);
    }
  }

  public void onResumeMaintenanceMode(HelixManager manager, Object eventInfo) {
    HelixAdmin admin = manager.getClusterManagmentTool();
    if (admin.isInMaintenanceMode(manager.getClusterName())) {
      admin.manuallyEnableMaintenanceMode(manager.getClusterName(), false,
          "Cloud event is completed: " + eventInfo.toString(), null);
    }
  }
}
