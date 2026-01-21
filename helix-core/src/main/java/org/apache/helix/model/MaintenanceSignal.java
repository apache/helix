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
import java.util.Arrays;
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
   * Add a new maintenance reason. If the triggering entity already has a reason, it will be replaced.
   *
   * @param reason The reason for maintenance
   * @param timestamp The timestamp when maintenance was triggered
   * @param triggeringEntity The entity that triggered maintenance
   */
  public void addMaintenanceReason(String reason, long timestamp, TriggeringEntity triggeringEntity) {
    LOG.info("Adding maintenance reason for entity: {}, reason: {}, timestamp: {}",
        triggeringEntity, reason, timestamp);

    List<Map<String, String>> reasons = getMaintenanceReasons();
    LOG.debug("Before addition: Reasons list contains {} entries", reasons.size());

    // Filter out any existing reasons for this triggering entity
    List<Map<String, String>> filteredReasons = filterReasons(reasons, null, 
        Arrays.asList(triggeringEntity));

    // Always add the new reason at the end of the list
    Map<String, String> newEntry = new HashMap<>();
    newEntry.put(PauseSignalProperty.REASON.name(), reason);
    newEntry.put(MaintenanceSignalProperty.TIMESTAMP.name(), Long.toString(timestamp));
    newEntry.put(MaintenanceSignalProperty.TRIGGERED_BY.name(), triggeringEntity.name());
    filteredReasons.add(newEntry);

    updateReasonsListField(filteredReasons);
    LOG.debug("After addition: Reasons list contains {} entries", filteredReasons.size());
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
    List<Map<String, String>> filteredReasons = filterReasons(reasons, null, 
        Arrays.asList(triggeringEntity));
    
    // Return false early if no entities were filtered out
    if (filteredReasons.size() == reasons.size()) {
      LOG.info("Entity {} doesn't have a maintenance reason entry, ignoring exit request", triggeringEntity);
      return false;
    }

    // Update reasons list field with filtered reasons
    updateReasonsListField(filteredReasons);

    // Always set/reset the simpleFields if filteredReasons.size() != 0
    if (!filteredReasons.isEmpty()) {
      // Get the most recent reason (last element, since list is in chronological order)
      Map<String, String> mostRecent = filteredReasons.get(filteredReasons.size() - 1);
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

    return true;
  }

  /**
   * Filter maintenance reasons based on include and exclude entity lists.
   *
   * @param reasons The original list of maintenance reasons
   * @param includeEntities List of entities to include (null means include all)
   * @param excludeEntities List of entities to exclude (null means exclude none)
   * @return Filtered list of maintenance reasons (maintains original order)
   */
  private List<Map<String, String>> filterReasons(List<Map<String, String>> reasons,
                                                  List<TriggeringEntity> includeEntities,
                                                  List<TriggeringEntity> excludeEntities) {
    List<Map<String, String>> filtered = new ArrayList<>();

    for (Map<String, String> reason : reasons) {
      String triggeredByStr = reason.get(MaintenanceSignalProperty.TRIGGERED_BY.name());
      if (triggeredByStr == null) {
        continue;
      }

      TriggeringEntity entity;
      try {
        entity = TriggeringEntity.valueOf(triggeredByStr);
      } catch (IllegalArgumentException ex) {
        LOG.warn("Unknown triggering entity: {}, skipping", triggeredByStr);
        continue;
      }

      if ((includeEntities != null && !includeEntities.contains(entity)) ||
          (excludeEntities != null && excludeEntities.contains(entity))) {
        continue;
      }

      filtered.add(reason);
    }

    return filtered;
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
    List<Map<String, String>> filteredReasons = filterReasons(reasons, 
        Arrays.asList(triggeringEntity), null);
    return !filteredReasons.isEmpty();
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

    // Early return if no simple reason exists, not a USER entity, or USER already has a reason
    List<Map<String, String>> reasons = getMaintenanceReasons();
    if (simpleReason == null || simpleEntity != TriggeringEntity.USER || 
        !filterReasons(reasons, Arrays.asList(TriggeringEntity.USER), null).isEmpty()) {
      return;
    }

    // Legacy USER data exists but not in listFields - preserve it
    Map<String, String> legacyEntry = new HashMap<>();
    legacyEntry.put(PauseSignalProperty.REASON.name(), simpleReason);
    legacyEntry.put(MaintenanceSignalProperty.TIMESTAMP.name(), String.valueOf(simpleTimestamp));
    legacyEntry.put(MaintenanceSignalProperty.TRIGGERED_BY.name(), simpleEntity.name());

    reasons.add(legacyEntry);
    updateReasonsListField(reasons);

    LOG.info("Reconciled legacy USER maintenance data: reason={}, timestamp={}",
        simpleReason, simpleTimestamp);
  }
}
