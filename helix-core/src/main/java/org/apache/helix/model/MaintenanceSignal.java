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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.IOException;

/**
 * A ZNode that signals that the cluster is in maintenance mode.
 */
public class MaintenanceSignal extends PauseSignal {
  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceSignal.class);

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
    AUTOMATION, // triggered by automation systems (like HelixACM)
    USER, // manually triggered by user
    UNKNOWN
  }

  /**
   * Reason for the maintenance mode being triggered automatically. This will allow checking more
   * efficient because it will check against the exact condition for which the cluster entered
   * maintenance mode. This field does not apply when triggered manually.
   */
  public enum AutoTriggerReason {
    @Deprecated // Replaced with MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS
    MAX_OFFLINE_INSTANCES_EXCEEDED,
    MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS,
    MAX_PARTITION_PER_INSTANCE_EXCEEDED,
    NOT_APPLICABLE // Not triggered automatically or automatically exiting maintenance mode
  }

  /**
   * Constant for the name of the reasons list field
   */
  private static final String REASONS_LIST_FIELD = "reasons";

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

  /**
   * Add a new maintenance reason (or update an existing one if the triggering entity already has a reason).
   *
   * @param reason The reason for maintenance
   * @param timestamp The timestamp when maintenance was triggered
   * @param triggeringEntity The entity that triggered maintenance
   */
  public void addMaintenanceReason(String reason, long timestamp, TriggeringEntity triggeringEntity) {
    LOG.info("Adding maintenance reason for entity: {}, reason: {}, timestamp: {}",
        triggeringEntity, reason, timestamp);

    // The triggering entity is our unique key - Overwrite any existing entry with this entity
    String triggerEntityStr = triggeringEntity.name();

    List<Map<String, String>> reasons = getMaintenanceReasons();
    LOG.debug("Before addition: Reasons list contains {} entries", reasons.size());

    boolean found = false;
    for (Map<String, String> entry : reasons) {
      if (triggerEntityStr.equals(entry.get(MaintenanceSignalProperty.TRIGGERED_BY.name()))) {
        entry.put(PauseSignalProperty.REASON.name(), reason);
        entry.put(MaintenanceSignalProperty.TIMESTAMP.name(), Long.toString(timestamp));
        found = true;
        LOG.debug("Updated existing entry for entity: {}", triggeringEntity);
        break;
      }
    }

    if (!found) {
      Map<String, String> newEntry = new HashMap<>();
      newEntry.put(PauseSignalProperty.REASON.name(), reason);
      newEntry.put(MaintenanceSignalProperty.TIMESTAMP.name(), Long.toString(timestamp));
      newEntry.put(MaintenanceSignalProperty.TRIGGERED_BY.name(), triggerEntityStr);
      reasons.add(newEntry);
      LOG.debug("Added new entry for entity: {}", triggeringEntity);
    }

    updateReasonsListField(reasons);
    LOG.debug("After addition: Reasons list contains {} entries", reasons.size());
  }

  /**
   * Helper method to update the ZNRecord with the current reasons list.
   * Each reason is stored as a single JSON string in the list.
   *
   * @param reasons The list of reason maps to store
   */
  private void updateReasonsListField(List<Map<String, String>> reasons) {
    List<String> reasonsList = new ArrayList<>();

    for (Map<String, String> entry : reasons) {
      String jsonString = convertMapToJsonString(entry);
      if (!jsonString.isEmpty()) {
        reasonsList.add(jsonString);
      }
    }

    _record.setListField(REASONS_LIST_FIELD, reasonsList);
  }

  /**
   * Convert a map to a JSON-style string
   */
  private String convertMapToJsonString(Map<String, String> map) {
    try {
      return new ObjectMapper().writeValueAsString(map);
    } catch (IOException e) {
      LOG.warn("Failed to convert map to JSON string: {}", e.getMessage());
      return "";
    }
  }

  /**
   * Get all maintenance reasons currently active.
   *
   * @return List of maintenance reasons as maps
   */
  public List<Map<String, String>> getMaintenanceReasons() {
    List<Map<String, String>> reasons = new ArrayList<>();
    List<String> reasonsList = _record.getListField(REASONS_LIST_FIELD);

    if (reasonsList != null && !reasonsList.isEmpty()) {
      for (String entryStr : reasonsList) {
        Map<String, String> entry = parseJsonStyleEntry(entryStr);
        if (!entry.isEmpty()) {
          reasons.add(entry);
        }
      }
    }

    return reasons;
  }

  /**
   * Parse an entry string in JSON format into a map
   */
  private Map<String, String> parseJsonStyleEntry(String entryStr) {
    Map<String, String> map = new HashMap<>();
    try {
        return new ObjectMapper().readValue(entryStr,
            TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, String.class));
      } catch (IOException e) {
        LOG.warn("Failed to parse JSON entry: {}, error: {}", entryStr, e.getMessage());
      }
    return map;
  }

  /**
   * Remove a maintenance reason by triggering entity.
   *
   * @param triggeringEntity The entity whose reason should be removed
   * @return true if a reason was removed, false otherwise
   */
  public boolean removeMaintenanceReason(TriggeringEntity triggeringEntity) {
    LOG.info("Removing maintenance reason for entity: {}", triggeringEntity);

    List<Map<String, String>> reasons = getMaintenanceReasons();

    boolean entityExists = false;
    for (Map<String, String> entry : reasons) {
      String entryEntity = entry.get(MaintenanceSignalProperty.TRIGGERED_BY.name());
      if (triggeringEntity.name().equals(entryEntity)) {
        entityExists = true;
        break;
      }
    }

    if (!entityExists) {
      LOG.info("Entity {} doesn't have a maintenance reason entry, ignoring exit request", triggeringEntity);
      return false;
    }

    int originalSize = reasons.size();
    LOG.debug("Before removal: Reasons list contains {} entries", reasons.size());

    List<Map<String, String>> updatedReasons = new ArrayList<>();
    String targetEntity = triggeringEntity.name();

    // Only keep reasons that don't match the triggering entity
    for (Map<String, String> entry : reasons) {
      String entryEntity = entry.get(MaintenanceSignalProperty.TRIGGERED_BY.name());
      if (!targetEntity.equals(entryEntity)) {
        updatedReasons.add(entry);
      } else {
        LOG.debug("Removing entry with reason: {} for entity: {}",
            entry.get(PauseSignalProperty.REASON.name()), entryEntity);
      }
    }

    boolean removed = updatedReasons.size() < originalSize;
    LOG.debug("After removal: Reasons list contains {} entries", updatedReasons.size());

    if (removed) {
      updateReasonsListField(updatedReasons);

      // Update the simpleFields with the most recent reason (for backward compatibility)
      if (!updatedReasons.isEmpty()) {
        // Sort by timestamp in descending order to get the most recent
        updatedReasons.sort((r1, r2) -> {
          long t1 = Long.parseLong(r1.get(MaintenanceSignalProperty.TIMESTAMP.name()));
          long t2 = Long.parseLong(r2.get(MaintenanceSignalProperty.TIMESTAMP.name()));
          return Long.compare(t2, t1);
        });

        Map<String, String> mostRecent = updatedReasons.get(0);
        String newReason = mostRecent.get(PauseSignalProperty.REASON.name());
        long newTimestamp = Long.parseLong(mostRecent.get(MaintenanceSignalProperty.TIMESTAMP.name()));
        TriggeringEntity newEntity = TriggeringEntity.valueOf(
            mostRecent.get(MaintenanceSignalProperty.TRIGGERED_BY.name()));

        LOG.info("Updated to most recent reason: {}, entity: {}, timestamp: {}",
            newReason, newEntity, newTimestamp);

        setReason(newReason);
        setTimestamp(newTimestamp);
        setTriggeringEntity(newEntity);
      }
    } else {
      LOG.info("No matching maintenance reason found for entity: {}", triggeringEntity);
    }

    return removed;
  }

  /**
   * Check if there are any active maintenance reasons.
   *
   * @return true if there are any reasons for maintenance, false otherwise
   */
  public boolean hasMaintenanceReasons() {
    return !getMaintenanceReasons().isEmpty();
  }

  /**
   * Checks if there is a maintenance reason from a specific triggering entity.
   *
   * @param triggeringEntity The entity to check
   * @return true if there is a maintenance reason from this entity
   */
  public boolean hasMaintenanceReason(TriggeringEntity triggeringEntity) {
    List<Map<String, String>> reasons = getMaintenanceReasons();
    for (Map<String, String> entry : reasons) {
      if (triggeringEntity.name().equals(entry.get(MaintenanceSignalProperty.TRIGGERED_BY.name()))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets the maintenance reason details for a specific triggering entity.
   *
   * @param triggeringEntity The entity to get reason details for
   * @return Map containing reason details, or null if not found
   */
  public Map<String, String> getMaintenanceReasonDetails(TriggeringEntity triggeringEntity) {
    List<Map<String, String>> reasons = getMaintenanceReasons();
    for (Map<String, String> entry : reasons) {
      if (triggeringEntity.name().equals(entry.get(MaintenanceSignalProperty.TRIGGERED_BY.name()))) {
        return entry;
      }
    }
    return null;
  }

  /**
   * Gets the number of active maintenance reasons.
   *
   * @return The count of active maintenance reasons
   */
  public int getMaintenanceReasonsCount() {
    return getMaintenanceReasons().size();
  }

  /**
   * Gets the maintenance reason from a specific triggering entity.
   *
   * @param triggeringEntity The entity to get reason for
   * @return The reason string, or null if not found
   */
  public String getMaintenanceReason(TriggeringEntity triggeringEntity) {
    Map<String, String> details = getMaintenanceReasonDetails(triggeringEntity);
    return details != null ? details.get(PauseSignalProperty.REASON.name()) : null;
  }

  /**
   * Reconcile legacy data from simpleFields into listFields.reasons if it's missing.
   * This preserves maintenance data written by old USER clients that only set simpleFields.
   *
   * NOTE: Only reconciles USER data, as:
   * - CONTROLLER is part of core Helix system and should use proper APIs
   * - AUTOMATION is new and has no legacy clients
   * - Only USER entities represent external legacy clients that may wipe data
   */
  public void reconcileLegacyData() {
    // Check if simpleFields exist but corresponding listFields entry is missing
    String simpleReason = getReason();
    TriggeringEntity simpleEntity = getTriggeringEntity();
    long simpleTimestamp = getTimestamp();

    // Only reconcile USER data from legacy clients
    // CONTROLLER and AUTOMATION should not have legacy data loss scenarios
    if (simpleReason != null && !simpleReason.isEmpty() && simpleEntity == TriggeringEntity.USER
        && simpleTimestamp > 0 && !hasMaintenanceReason(simpleEntity)) {

      // Legacy USER data exists but not in listFields - preserve it
      Map<String, String> legacyEntry = new HashMap<>();
      legacyEntry.put(PauseSignalProperty.REASON.name(), simpleReason);
      legacyEntry.put(MaintenanceSignalProperty.TIMESTAMP.name(), String.valueOf(simpleTimestamp));
      legacyEntry.put(MaintenanceSignalProperty.TRIGGERED_BY.name(), simpleEntity.name());

      List<Map<String, String>> reasons = getMaintenanceReasons();
      reasons.add(legacyEntry);
      updateReasonsListField(reasons);

      LOG.info("Reconciled legacy USER maintenance data: reason={}, timestamp={}",
          simpleReason, simpleTimestamp);
    }
  }
}
