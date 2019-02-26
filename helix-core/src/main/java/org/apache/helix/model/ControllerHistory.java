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

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.codehaus.jackson.map.ObjectMapper;

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
    REASON

  }

  private enum OperationType {
    // The following are options for OPERATION_TYPE in MaintenanceConfigKey
    ENTER,
    EXIT
  }

  public enum HistoryType {
    CONTROLLER_LEADERSHIP,
    MAINTENANCE
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

    List<String> historyList = _record.getListField(ConfigProperty.HISTORY.name());
    if (historyList == null) {
      historyList = new ArrayList<>();
      _record.setListField(ConfigProperty.HISTORY.name(), historyList);
    }

    // Keep only the last HISTORY_SIZE entries
    while (historyList.size() >= HISTORY_SIZE) {
      historyList.remove(0);
    }

    Map<String, String> historyEntry = new HashMap<>();

    long currentTime = System.currentTimeMillis();
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    String dateTime = df.format(new Date(currentTime));

    historyEntry.put(ConfigProperty.CONTROLLER.name(), instanceName);
    historyEntry.put(ConfigProperty.TIME.name(), String.valueOf(currentTime));
    historyEntry.put(ConfigProperty.DATE.name(), dateTime);
    historyEntry.put(ConfigProperty.VERSION.name(), version);

    historyList.add(historyEntry.toString());
    return _record;
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
   * Record up to MAINTENANCE_HISTORY_SIZE number of changes to MaintenanceSignal in FIFO order.
   * @param enabled
   * @param reason
   * @param currentTime
   * @param internalReason
   * @param customFields
   * @param triggeringEntity
   */
  public ZNRecord updateMaintenanceHistory(boolean enabled, String reason, long currentTime,
      MaintenanceSignal.AutoTriggerReason internalReason, Map<String, String> customFields,
      MaintenanceSignal.TriggeringEntity triggeringEntity) throws IOException {
    List<String> maintenanceHistoryList =
        _record.getListField(MaintenanceConfigKey.MAINTENANCE_HISTORY.name());
    if (maintenanceHistoryList == null) {
      maintenanceHistoryList = new ArrayList<>();
      _record.setListField(MaintenanceConfigKey.MAINTENANCE_HISTORY.name(), maintenanceHistoryList);
    }

    // Keep only the last MAINTENANCE_HISTORY_SIZE entries
    while (maintenanceHistoryList.size() >= MAINTENANCE_HISTORY_SIZE) {
      maintenanceHistoryList.remove(0);
    }

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
    maintenanceHistoryList.add(new ObjectMapper().writeValueAsString(maintenanceEntry));
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
