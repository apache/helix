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

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.HelixVersion;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ProcId;
import org.apache.helix.api.id.SessionId;
import org.apache.log4j.Logger;

/**
 * Instance that connects to zookeeper (stored ephemerally)
 */
public class LiveInstance extends HelixProperty {
  /**
   * Saved properties of a live instance
   */
  public enum LiveInstanceProperty {
    SESSION_ID,
    HELIX_VERSION,
    LIVE_INSTANCE,
    ZKPROPERTYTRANSFERURL
  }

  private static final Logger _logger = Logger.getLogger(LiveInstance.class.getName());

  /**
   * Instantiate with an instance identifier
   * @param id instance identifier
   */
  public LiveInstance(String id) {
    super(id);
  }

  /**
   * Instantiate with an participant identifier
   * @param id participant identifier
   */
  public LiveInstance(ParticipantId id) {
    super(id.stringify());
  }

  /**
   * Instantiate with a pre-populated record
   * @param record ZNRecord corresponding to a live instance
   */
  public LiveInstance(ZNRecord record) {
    super(record);
  }

  /**
   * Set the session that this instance corresponds to
   * @param sessionId session identifier
   */
  public void setSessionId(String sessionId) {
    _record.setSimpleField(LiveInstanceProperty.SESSION_ID.toString(), sessionId);
  }

  /**
   * Get the session that this participant corresponds to
   * @return session identifier
   */
  public SessionId getTypedSessionId() {
    return SessionId.from(getSessionId());
  }

  /**
   * Get the session that this participant corresponds to
   * @return session identifier
   */
  public String getSessionId() {
    return _record.getSimpleField(LiveInstanceProperty.SESSION_ID.toString());
  }

  /**
   * Get the name of this instance
   * @return the instance name
   */
  public String getInstanceName() {
    return _record.getId();
  }

  /**
   * Get the id of this participant
   * @return participant id
   */
  public ParticipantId getParticipantId() {
    return ParticipantId.from(getInstanceName());
  }

  /**
   * Get the version of Helix that this participant is running
   * @return the version
   */
  public HelixVersion getTypedHelixVersion() {
    return HelixVersion.from(getHelixVersion());
  }

  /**
   * Get the version of Helix that this participant is running
   * @return the version
   */
  public String getHelixVersion() {
    return _record.getSimpleField(LiveInstanceProperty.HELIX_VERSION.toString());
  }

  /**
   * Set the version of Helix that this instance is running
   * @param helixVersion the version
   */
  public void setHelixVersion(String helixVersion) {
    _record.setSimpleField(LiveInstanceProperty.HELIX_VERSION.toString(), helixVersion);
  }

  /**
   * Get an identifier that represents the instance and where it is located
   * @return identifier, e.g. process_id@host
   */
  public String getLiveInstance() {
    return _record.getSimpleField(LiveInstanceProperty.LIVE_INSTANCE.toString());
  }

  /**
   * Get an identifier that represents the instance and where it is located
   * @return process identifier
   */
  public ProcId getProcessId() {
    return ProcId.from(getLiveInstance());
  }

  /**
   * Set an identifier that represents the process
   * @param liveInstance process identifier, e.g. process_id@host
   */
  public void setLiveInstance(String liveInstance) {
    _record.setSimpleField(LiveInstanceProperty.LIVE_INSTANCE.toString(), liveInstance);
  }

  /**
   * Get the last modified time of this live instance
   * @return UNIX timestamp
   */
  public long getModifiedTime() {
    return _record.getModifiedTime();
  }

  /**
   * Get a web service URL where ZK properties can be transferred to
   * @return a fully-qualified URL
   */
  public String getWebserviceUrl() {
    return _record.getSimpleField(LiveInstanceProperty.ZKPROPERTYTRANSFERURL.toString());
  }

  /**
   * Set a web service URL where ZK properties can be transferred to
   * @param url a fully-qualified URL
   */
  public void setWebserviceUrl(String url) {
    _record.setSimpleField(LiveInstanceProperty.ZKPROPERTYTRANSFERURL.toString(), url);
  }

  @Override
  public boolean isValid() {
    if (getTypedSessionId() == null) {
      _logger.error("liveInstance does not have session id. id:" + _record.getId());
      return false;
    }
    if (getTypedHelixVersion() == null) {
      _logger.error("liveInstance does not have CLM verion. id:" + _record.getId());
      return false;
    }
    return true;
  }
}
