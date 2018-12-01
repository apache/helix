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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    ZKPROPERTYTRANSFERURL,
    RESOURCE_CAPACITY
  }

  /**
   * Resource this instance can provide, i.e. thread, memory heap size, CPU cores, etc
   */
  public enum InstanceResourceType {
    TASK_EXEC_THREAD
  }

  private static final Logger _logger = LoggerFactory.getLogger(LiveInstance.class.getName());

  /**
   * Instantiate with an instance identifier
   * @param id instance identifier
   */
  public LiveInstance(String id) {
    super(id);
  }

  /**
   * Instantiate with a pre-populated record
   * @param record ZNRecord corresponding to a live instance
   */
  public LiveInstance(ZNRecord record) {
    super(record);
  }

  /**
   * TODO Deprecate this method since session Id should always read from node stat. It should be readonly. -- JJ
   * Set the session that this instance corresponds to
   * @param sessionId session identifier
   */
  public void setSessionId(String sessionId) {
    _record.setSimpleField(LiveInstanceProperty.SESSION_ID.toString(), sessionId);
  }

  /**
   * TODO Read the session Id directly from node stat. -- JJ
   * Get the session that this instance corresponds to
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
   * Get the version of Helix that this instance is running
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
   * Set an identifier that represents the process
   * @param liveInstance process identifier, e.g. process_id@host
   */
  public void setLiveInstance(String liveInstance) {
    _record.setSimpleField(LiveInstanceProperty.LIVE_INSTANCE.toString(), liveInstance);
  }

  /**
   * Get resource quota map of the live instance. Note that this resource name
   * refers to compute / storage / network resource that this liveinstance
   * has, i.e. thread count, CPU cores, heap size, etc.
   * @return resource quota map: key=resourceName, value=quota
   */
  public Map<String, String> getResourceCapacityMap() {
    return _record.getMapField(LiveInstanceProperty.RESOURCE_CAPACITY.name());
  }

  /**
   * Add a resource quota map to this LiveInstance. For resource quota map, key=resourceName;
   * value=quota of that resource. We assume that value can be casted into integers. Note that
   * this resourceName refers to compute / storage / network resource that this liveinstance
   * has, i.e. thread count, CPU cores, heap size, etc.
   *
   * @param resourceQuotaMap resourceQuotaMap
   */
  public void setResourceCapacityMap(Map<String, String> resourceQuotaMap) {
    _record.setMapField(LiveInstanceProperty.RESOURCE_CAPACITY.name(), resourceQuotaMap);
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
    if (getSessionId() == null) {
      _logger.error("liveInstance does not have session id. id:" + _record.getId());
      return false;
    }
    if (getHelixVersion() == null) {
      _logger.error("liveInstance does not have CLM verion. id:" + _record.getId());
      return false;
    }
    return true;
  }
}
