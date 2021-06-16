package org.apache.helix.model;

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

import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyType;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Represents the cluster status. It can have fields for
 * {@link ClusterManagementMode} type and status.
 */
public class ClusterStatus extends HelixProperty {
  public ClusterStatus() {
    super(PropertyType.STATUS.name());
  }

  public ClusterStatus(ZNRecord record) {
    super(record);
  }

  public enum ClusterStatusProperty {
    MANAGEMENT_MODE,
    MANAGEMENT_MODE_STATUS
  }

  /**
   * Sets the type of management mode
   *
   * @param mode {@link ClusterManagementMode.Type}
   */
  public void setManagementMode(ClusterManagementMode.Type mode) {
    _record.setEnumField(ClusterStatusProperty.MANAGEMENT_MODE.name(), mode);
  }

  /**
   * Gets the type of management mode
   *
   * @return {@link ClusterManagementMode.Type}
   */
  public ClusterManagementMode.Type getManagementMode() {
    return _record.getEnumField(ClusterStatusProperty.MANAGEMENT_MODE.name(),
        ClusterManagementMode.Type.class, null);
  }

  /**
   * Sets the cluster management mode status.
   *
   * @param status {@link ClusterManagementMode.Status}
   */
  public void setManagementModeStatus(ClusterManagementMode.Status status) {
    _record.setEnumField(ClusterStatusProperty.MANAGEMENT_MODE_STATUS.name(), status);
  }

  /**
   * Gets the {@link ClusterManagementMode.Status} of cluster management mode.
   *
   * @return {@link ClusterManagementMode.Status} if status is valid; otherwise, return {@code
   * null}.
   */
  public ClusterManagementMode.Status getManagementModeStatus() {
    return _record.getEnumField(ClusterStatusProperty.MANAGEMENT_MODE_STATUS.name(),
        ClusterManagementMode.Status.class, null);
  }
}