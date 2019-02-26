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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.ZNRecord;

/**
 * A ZNode that signals that the cluster is in maintenance mode.
 */
public class MaintenanceSignal extends PauseSignal {

  /**
   * Pre-defined fields set by Helix Controller only.
   */
  public enum MaintenanceSignalProperty {
    TRIGGERED_BY,
    TIMESTAMP,
    AUTO_TRIGGER_REASON
  }

  /**
   * Possible values for TRIGGERED_BY field in MaintenanceSignal.
   */
  public enum TriggeringEntity {
    CONTROLLER,
    USER, // manually triggered by user
    UNKNOWN
  }

  /**
   * Reason for the maintenance mode being triggered automatically. This will allow checking more
   * efficient because it will check against the exact condition for which the cluster entered
   * maintenance mode. This field does not apply when triggered manually.
   */
  public enum AutoTriggerReason {
    MAX_OFFLINE_INSTANCES_EXCEEDED,
    MAX_PARTITION_PER_INSTANCE_EXCEEDED,
    NOT_APPLICABLE // Not triggered automatically or automatically exiting maintenance mode
  }

  public MaintenanceSignal(String id) {
    super(id);
  }

  public MaintenanceSignal(ZNRecord record) {
    super(record);
  }

  public void setTriggeringEntity(TriggeringEntity triggeringEntity) {
    _record.setSimpleField(MaintenanceSignalProperty.TRIGGERED_BY.name(), triggeringEntity.name());
  }

  /**
   * Returns triggering entity.
   * @return TriggeringEntity.UNKNOWN if the field does not exist.
   */
  public TriggeringEntity getTriggeringEntity() {
    try {
      return TriggeringEntity
          .valueOf(_record.getSimpleField(MaintenanceSignalProperty.TRIGGERED_BY.name()));
    } catch (Exception e) {
      return TriggeringEntity.UNKNOWN;
    }
  }

  public void setAutoTriggerReason(AutoTriggerReason internalReason) {
    _record.setSimpleField(MaintenanceSignalProperty.AUTO_TRIGGER_REASON.name(),
        internalReason.name());
  }

  /**
   * Returns auto-trigger reason.
   * @return AutoTriggerReason.NOT_APPLICABLE if it was not triggered automatically
   */
  public AutoTriggerReason getAutoTriggerReason() {
    try {
      return AutoTriggerReason
          .valueOf(_record.getSimpleField(MaintenanceSignalProperty.AUTO_TRIGGER_REASON.name()));
    } catch (Exception e) {
      return AutoTriggerReason.NOT_APPLICABLE;
    }
  }

  public void setTimestamp(long timestamp) {
    _record.setLongField(MaintenanceSignalProperty.TIMESTAMP.name(), timestamp);
  }

  /**
   * Returns last modified time.
   * TODO: Consider using modifiedTime in ZK Stat object.
   * @return -1 if the field does not exist.
   */
  public long getTimestamp() {
    return _record.getLongField(MaintenanceSignalProperty.TIMESTAMP.name(), -1);
  }
}
