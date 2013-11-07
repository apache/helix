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
import org.apache.helix.api.id.SessionId;

/**
 * Describe alerts and corresponding metrics. An alert is triggered when cluster health
 * violates pre-defined constraints to ensure that the cluster meets SLAs.
 */
public class Alerts extends HelixProperty {

  // private final ZNRecord _record;

  /**
   * The name of the ZNode corresponding to this property
   */
  public final static String nodeName = "Alerts";

  /**
   * Supported fields corresponding to a set of alerts
   */
  public enum AlertsProperty {
    SESSION_ID,
    FIELDS
  }

  // private final ZNRecord _record;

  /**
   * Instantiate with an identifier
   * @param id A string that identifies the alerts
   */
  public Alerts(String id) {
    super(id);
  }

  /**
   * Instantiate with a pre-populated Alerts record
   * @param record ZNRecord representing Alerts
   */
  public Alerts(ZNRecord record) {
    // _record = record;
    super(record);

  }

  /*
   * public Alerts(ZNRecord record, Stat stat) { super(record, stat); }
   */

  /**
   * Set the session that the alerts correspond to
   * @param sessionId the session for which alerts occurred
   */
  public void setSessionId(String sessionId) {
    _record.setSimpleField(AlertsProperty.SESSION_ID.toString(), sessionId);
  }

  /**
   * Set the session that the alerts correspond to
   * @param sessionId the session for which alerts occurred
   */
  public void setSessionId(SessionId sessionId) {
    if (sessionId != null) {
      setSessionId(sessionId.stringify());
    }
  }

  /**
   * Get the session that the alerts correspond to
   * @return session identifier
   */
  public String getSessionId() {
    return _record.getSimpleField(AlertsProperty.SESSION_ID.toString());
  }

  /**
   * Get the session that the alerts correspond to
   * @return session identifier
   */
  public SessionId getTypedSessionId() {
    return SessionId.from(getSessionId());
  }

  /**
   * Get the instance that the alerts correspond to
   * @return the name of the instance
   */
  public String getInstanceName() {
    return _record.getId();
  }

  /*
   * public String getVersion() { return
   * _record.getSimpleField(AlertsProperty.CLUSTER_MANAGER_VERSION.toString());
   * }
   */

  /**
   * Get the alerts
   * @return a mapping of alert stat name to alert properties
   */
  public Map<String, Map<String, String>> getMapFields() {
    return _record.getMapFields();
  }

  /**
   * Get specific alert statistics
   * @param statName the name of the statistic group
   * @return a mapping of property and value for the statistic
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
