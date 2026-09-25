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

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * The history of instances that have served as the leader controller
 */
public class ControllerHistory extends HelixProperty {
  private final static int HISTORY_SIZE = 10;
  private final static int MAINTENANCE_HISTORY_SIZE = 20;

  private enum ConfigProperty {
    HISTORY,
    TIME,
    DATE,
    VERSION,
    CONTROLLER
  }

  private enum MaintenanceConfigKey {
    MAINTENANCE_HISTORY,
    OPERATION_TYPE,
    DATE,
    REASON,
    IN_MAINTENANCE_AFTER_OPERATION
  }

  private enum ManagementModeConfigKey {
    MANAGEMENT_MODE_HISTORY,
    MODE,
    STATUS
  }

  private enum OperationType {
    // The following are options for OPERATION_TYPE in MaintenanceConfigKey
    ENTER,
    EXIT
  }

  public enum HistoryType {
    CONTROLLER_LEADERSHIP,
    MAINTENANCE,
    MANAGEMENT_MODE
  }

  public ControllerHistory(String id) {
    super(id);
  }

  public ControllerHistory(ZNRecord record) {
    super(record);
  }

  /**
   * Save up to HISTORY_SIZE number of leaders in FIFO order
   * @param clusterName the cluster the instance leads
   * @param instanceName the name of the leader instance
   */
  public ZNRecord updateHistory(String clusterName, String instanceName, String version) {
    /* keep this for back-compatible */
    // TODO: remove this in future when we confirmed no one consumes it
    List<String> list = _record.getListField(clusterName);
    if (list == null) {
      list = new ArrayList<>();
      _record.setListField(clusterName, list);
    }

    while (list.size() >= HISTORY_SIZE) {
      list.remove(0);
    }
    list.add(instanceName);
    // TODO: remove above in future when we confirmed no one consumes it */

    Map<String, String> historyEntry = new HashMap<>();

    long currentTime = System.currentTimeMillis();
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    String dateTime = df.format(new Date(currentTime));

    historyEntry.put(ConfigProperty.CONTROLLER.name(), instanceName);
    historyEntry.put(ConfigProperty.TIME.name(), String.valueOf(currentTime));
    historyEntry.put(ConfigProperty.DATE.name(), dateTime);
    historyEntry.put(ConfigProperty.VERSION.name(), version);

    return populateHistoryEntries(HistoryType.CONTROLLER_LEADERSHIP, historyEntry.toString());
  }

  /**
   * Get history list
   * @return
   */
  public List<String> getHistoryList() {
    List<String> historyList = _record.getListField(ConfigProperty.HISTORY.name());
    if (historyList == null) {
      historyList = new ArrayList<>();
    }

    return historyList;
  }

  /**
   * Gets the management mode history.
   *
   * @return List of history strings.
   */
  public List<String> getManagementModeHistory() {
    List<String> history =
        _record.getListField(ManagementModeConfigKey.MANAGEMENT_MODE_HISTORY.name());
    return history == null ? Collections.emptyList() : history;
  }

  /**
   * Updates management mode and status history to controller history in FIFO order.
   *
   * @param controller controller name
   * @param mode cluster management mode {@link ClusterManagementMode}
   * @param fromHost the hostname that creates the management mode signal
   * @param time time in millis
   * @param reason reason to put the cluster in management mode
   * @return updated history znrecord
   */
  public ZNRecord updateManagementModeHistory(String controller, ClusterManagementMode mode,
      String fromHost, long time, String reason) {
    Map<String, String> historyEntry = new HashMap<>();
    historyEntry.put(ConfigProperty.CONTROLLER.name(), controller);
    historyEntry.put(ConfigProperty.TIME.name(), Instant.ofEpochMilli(time).toString());
    historyEntry.put(ManagementModeConfigKey.MODE.name(), mode.getMode().name());
    historyEntry.put(ManagementModeConfigKey.STATUS.name(), mode.getStatus().name());
    if (fromHost != null) {
      historyEntry.put(PauseSignal.PauseSignalProperty.FROM_HOST.name(), fromHost);
    }
    if (reason != null) {
      historyEntry.put(PauseSignal.PauseSignalProperty.REASON.name(), reason);
    }

    return populateHistoryEntries(HistoryType.MANAGEMENT_MODE, historyEntry.toString());
  }

  /**
   * Record up to MAINTENANCE_HISTORY_SIZE number of changes to MaintenanceSignal in FIFO order.
   * @param enabled
   * @param reason
   * @param currentTime
   * @param internalReason
   * @param customFields
   * @param triggeringEntity
   * @param inMaintenanceAfterOperation whether the cluster is still in maintenance mode after this operation
   */
  public ZNRecord updateMaintenanceHistory(boolean enabled, String reason, long currentTime,
      MaintenanceSignal.AutoTriggerReason internalReason, Map<String, String> customFields,
      MaintenanceSignal.TriggeringEntity triggeringEntity, boolean inMaintenanceAfterOperation) throws IOException {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH:" + "mm:ss");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    String dateTime = df.format(new Date(currentTime));

    // Populate maintenanceEntry
    Map<String, String> maintenanceEntry = new HashMap<>();
    maintenanceEntry.put(MaintenanceConfigKey.OPERATION_TYPE.name(),
        enabled ? OperationType.ENTER.name() : OperationType.EXIT.name());
    maintenanceEntry.put(MaintenanceConfigKey.REASON.name(), reason);
    maintenanceEntry.put(MaintenanceConfigKey.DATE.name(), dateTime);
    maintenanceEntry.put(MaintenanceSignal.MaintenanceSignalProperty.TIMESTAMP.name(),
        String.valueOf(currentTime));
    maintenanceEntry.put(MaintenanceSignal.MaintenanceSignalProperty.TRIGGERED_BY.name(),
        triggeringEntity.name());
    maintenanceEntry.put(MaintenanceConfigKey.IN_MAINTENANCE_AFTER_OPERATION.name(),
        String.valueOf(inMaintenanceAfterOperation));
    if (triggeringEntity == MaintenanceSignal.TriggeringEntity.CONTROLLER) {
      // If auto-triggered
      maintenanceEntry.put(MaintenanceSignal.MaintenanceSignalProperty.AUTO_TRIGGER_REASON.name(),
          internalReason.name());
    } else {
      // If manually triggered
      if (customFields != null && !customFields.isEmpty()) {
        for (Map.Entry<String, String> customFieldEntry : customFields.entrySet()) {
          if (!maintenanceEntry.containsKey(customFieldEntry.getKey())) {
            // Make sure custom entries do not overwrite pre-defined fields
            maintenanceEntry.put(customFieldEntry.getKey(), customFieldEntry.getValue());
          }
        }
      }
    }

    return populateHistoryEntries(HistoryType.MAINTENANCE,
        new ObjectMapper().writeValueAsString(maintenanceEntry));
  }

  private ZNRecord populateHistoryEntries(HistoryType type, String entry) {
    String configKey;
    int historySize;
    switch (type) {
      case CONTROLLER_LEADERSHIP:
        configKey = ConfigProperty.HISTORY.name();
        historySize = HISTORY_SIZE;
        break;
      case MAINTENANCE:
        configKey = MaintenanceConfigKey.MAINTENANCE_HISTORY.name();
        historySize = MAINTENANCE_HISTORY_SIZE;
        break;
      case MANAGEMENT_MODE:
        configKey = ManagementModeConfigKey.MANAGEMENT_MODE_HISTORY.name();
        historySize = HISTORY_SIZE;
        break;
      default:
        throw new HelixException("Unknown history type " + type.name());
    }

    List<String> historyList = _record.getListField(configKey);
    if (historyList == null) {
      historyList = new ArrayList<>();
      _record.setListField(configKey, historyList);
    }

    while (historyList.size() >= historySize) {
      historyList.remove(0);
    }

    historyList.add(entry);

    return _record;
  }

  /**
   * Get maintenance history list.
   * @return
   */
  public List<String> getMaintenanceHistoryList() {
    List<String> maintenanceHistoryList =
        _record.getListField(MaintenanceConfigKey.MAINTENANCE_HISTORY.name());
    if (maintenanceHistoryList == null) {
      maintenanceHistoryList = new ArrayList<>();
    }
    return maintenanceHistoryList;
  }

  @Override
  public boolean isValid() {
    return true;
  }
}
