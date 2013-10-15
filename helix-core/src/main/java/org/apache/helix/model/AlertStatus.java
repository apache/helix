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
import org.apache.helix.model.Alerts.AlertsProperty;

/**
 * Get characteristics of alerts, whether they were fired, and their context. An alert is triggered
 * when cluster health violates pre-defined constraints to ensure that the cluster meets SLAs.
 */
public class AlertStatus extends HelixProperty {

  /**
   * The name of the ZNode containing alert status
   */
  public final static String nodeName = "AlertStatus";

  /**
   * Instantiate with an identifier
   * @param id identifier representing this group of alert statuses
   */
  public AlertStatus(String id) {
    super(id);
  }

  /**
   * Instantiate with a pre-populated record corresponding to alert status
   * @param record ZNRecord representing alert statuses
   */
  public AlertStatus(ZNRecord record) {
    // _record = record;
    super(record);

  }

  /*
   * public Alerts(ZNRecord record, Stat stat) { super(record, stat); }
   */

  /**
   * Set the session that these alerts correspond to
   * @param sessionId session for which to look up alerts
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
   * Get the session that these alerts correspond to
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
   * Get the instance that these alerts correspond to
   * @return name of the instance
   */
  public String getInstanceName() {
    return _record.getId();
  }

  /*
   * public String getVersion() { return
   * _record.getSimpleField(AlertsProperty.CLUSTER_MANAGER_VERSION.toString()); }
   */

  /**
   * Get the properties of all alerts, such as if they were fired
   * @return all alert statuses as a Map of alert to the status properties
   */
  public Map<String, Map<String, String>> getMapFields() {
    return _record.getMapFields();
  }

  /**
   * Get the statistics of a single alert
   * @param statName Name of the alert
   * @return alert statistics as a map of name, value pairs
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
