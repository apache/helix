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

import java.util.Map;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.log4j.Logger;

/**
 * Statistics for an instance
 */
public class PersistentStats extends HelixProperty {
  private static final Logger _logger = Logger.getLogger(PersistentStats.class.getName());

  /**
   * High-level properties to provide context for these statistics
   */
  public enum PersistentStatsProperty {
    SESSION_ID,
    FIELDS
  }

  // private final ZNRecord _record;

  /**
   * The name of the statistics ZNode
   */
  public final static String nodeName = "PersistentStats";

  /**
   * Instantiate with an identifier
   * @param id record identifier
   */
  public PersistentStats(String id) {
    super(id);
  }

  /**
   * Instantiate with a pre-populated record
   * @param record ZNRecord with fields corresponding to persistent stats
   */
  public PersistentStats(ZNRecord record) {
    // _record = record;
    super(record);

  }

  /*
   * public PersistentStats(ZNRecord record, Stat stat)
   * {
   * super(record, stat);
   * }
   */

  /**
   * Set the session corresponding to these statistics
   * @param sessionId session id
   */
  public void setSessionId(String sessionId) {
    _record.setSimpleField(PersistentStatsProperty.SESSION_ID.toString(), sessionId);
  }

  /**
   * Get the session corresponding to these statistics
   * @return session id
   */
  public String getSessionId() {
    return _record.getSimpleField(PersistentStatsProperty.SESSION_ID.toString());
  }

  /**
   * Get the instance for which these stats have been collected
   * @return instance name
   */
  public String getInstanceName() {
    return _record.getId();
  }

  /*
   * public String getVersion()
   * {
   * return _record.getSimpleField(CLUSTER_MANAGER_VERSION.toString());
   * }
   */

  /**
   * Get all the statistics currently stored
   * @return map of (stat name, stat attribute, value)
   */
  public Map<String, Map<String, String>> getMapFields() {
    return _record.getMapFields();
  }

  /**
   * Get a specific statistic
   * @param statName the statistic to look up
   * @return map of (stat attribute, value)
   */
  public Map<String, String> getStatFields(String statName) {
    return _record.getMapField(statName);
  }

  @Override
  public boolean isValid() {
    // TODO Auto-generated method stub
    return true;
  }

}
