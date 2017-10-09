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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Current states of partitions in a resource for an instance.
 */
public class CurrentState extends HelixProperty {
  private static Logger LOG = LoggerFactory.getLogger(CurrentState.class);

  /**
   * Lookup keys for the current state
   */
  public enum CurrentStateProperty {
    SESSION_ID,
    CURRENT_STATE,
    REQUESTED_STATE,
    INFO,
    STATE_MODEL_DEF,
    STATE_MODEL_FACTORY_NAME,
    RESOURCE,
    START_TIME,
    END_TIME,
    PREVIOUS_STATE,
    TRIGGERED_BY// ,
             // BUCKET_SIZE
  }

  /**
   * Instantiate a current state with a resource
   * @param resourceName name identifying the resource
   */
  public CurrentState(String resourceName) {
    super(resourceName);
  }

  /**
   * Instantiate a current state with a pre-populated ZNRecord
   * @param record a ZNRecord corresponding to the current state
   */
  public CurrentState(ZNRecord record) {
    super(record);
  }

  /**
   * Get the name of the resource
   * @return String resource identifier
   */
  public String getResourceName() {
    return _record.getId();
  }

  /**
   * Get the partitions on this instance and the state that each partition is currently in.
   * @return (partition, state) pairs
   */
  public Map<String, String> getPartitionStateMap() {
    Map<String, String> map = new HashMap<String, String>();
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    for (String partitionName : mapFields.keySet()) {
      Map<String, String> tempMap = mapFields.get(partitionName);
      if (tempMap != null) {
        map.put(partitionName, tempMap.get(CurrentStateProperty.CURRENT_STATE.name()));
      }
    }
    return map;
  }

  /**
   * Get the session that this current state corresponds to
   * @return String session identifier
   */
  public String getSessionId() {
    return _record.getSimpleField(CurrentStateProperty.SESSION_ID.name());
  }

  /**
   * Set the session that this current state corresponds to
   * @param sessionId String session identifier
   */
  public void setSessionId(String sessionId) {
    _record.setSimpleField(CurrentStateProperty.SESSION_ID.name(), sessionId);
  }

  /**
   * Get the state of a partition on this instance
   * @param partitionName the name of the partition
   * @return the state, or null if the partition is not present
   */
  public String getState(String partitionName) {
    return getProperty(partitionName, CurrentStateProperty.CURRENT_STATE);
  }

  public String getInfo(String partitionName) {
    return getProperty(partitionName, CurrentStateProperty.INFO);
  }

  public String getRequestedState(String partitionName) {
    return getProperty(partitionName, CurrentStateProperty.REQUESTED_STATE);
  }

  public long getStartTime(String partitionName) {
    String startTime = getProperty(partitionName, CurrentStateProperty.START_TIME);
    return startTime == null ? -1L : Long.parseLong(startTime);
  }

  public long getEndTime(String partitionName) {
    String endTime = getProperty(partitionName, CurrentStateProperty.END_TIME);
    return endTime == null ? -1L : Long.parseLong(endTime);
  }

  public String getTriggerHost(String partitionName) {
    return getProperty(partitionName, CurrentStateProperty.TRIGGERED_BY);
  }

  public String getPreviousState(String partitionName) {
    return getProperty(partitionName, CurrentStateProperty.PREVIOUS_STATE);
  }

  private String getProperty(String partitionName, CurrentStateProperty property) {
    Map<String, String> mapField = _record.getMapField(partitionName);
    if (mapField != null) {
      return mapField.get(property.name());
    }
    return null;
  }

  /**
   * Set the state model that the resource follows
   * @param stateModelName an identifier of the state model
   */
  public void setStateModelDefRef(String stateModelName) {
    _record.setSimpleField(CurrentStateProperty.STATE_MODEL_DEF.name(), stateModelName);
  }

  /**
   * Get the state model that the resource follows
   * @return an identifier of the state model
   */
  public String getStateModelDefRef() {
    return _record.getSimpleField(CurrentStateProperty.STATE_MODEL_DEF.name());
  }

  /**
   * Set the state that a partition is currently in on this instance
   * @param partitionName the name of the partition
   * @param state the state of the partition
   */
  public void setState(String partitionName, String state) {
    setProperty(partitionName, CurrentStateProperty.CURRENT_STATE, state);
  }

  public void setInfo(String partitionName, String info) {
    setProperty(partitionName, CurrentStateProperty.INFO, info);
  }

  public void setRequestedState(String partitionName, String state) {
    setProperty(partitionName, CurrentStateProperty.REQUESTED_STATE, state);
  }

  public void setStartTime(String partitionName, long startTime) {
    setProperty(partitionName, CurrentStateProperty.START_TIME, String.valueOf(startTime));
  }

  public void setEndTime(String partitionName, long endTime) {
    setProperty(partitionName, CurrentStateProperty.END_TIME, String.valueOf(endTime));
  }

  public void setTriggerHost(String partitionName, String triggerHost) {
    setProperty(partitionName, CurrentStateProperty.TRIGGERED_BY, triggerHost);
  }

  public void setPreviousState(String partitionName, String state) {
    setProperty(partitionName, CurrentStateProperty.PREVIOUS_STATE, state);
  }

  private void setProperty(String partitionName, CurrentStateProperty property, String value) {
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    if (mapFields.get(partitionName) == null) {
      mapFields.put(partitionName, new TreeMap<String, String>());
    }
    mapFields.get(partitionName).put(property.name(), value);
  }

  /**
   * Set the state model factory
   * @param factoryName the name of the factory
   */
  public void setStateModelFactoryName(String factoryName) {
    _record.setSimpleField(CurrentStateProperty.STATE_MODEL_FACTORY_NAME.name(), factoryName);
  }

  /**
   * Get the state model factory
   * @return a name that identifies the state model factory
   */
  public String getStateModelFactoryName() {
    return _record.getSimpleField(CurrentStateProperty.STATE_MODEL_FACTORY_NAME.name());
  }

  // @Override
  // public int getBucketSize()
  // {
  // String bucketSizeStr =
  // _record.getSimpleField(CurrentStateProperty.BUCKET_SIZE.toString());
  // int bucketSize = 0;
  // if (bucketSizeStr != null)
  // {
  // try
  // {
  // bucketSize = Integer.parseInt(bucketSizeStr);
  // }
  // catch (NumberFormatException e)
  // {
  // // OK
  // }
  // }
  // return bucketSize;
  // }
  //
  // @Override
  // public void setBucketSize(int bucketSize)
  // {
  // if (bucketSize > 0)
  // {
  // _record.setSimpleField(CurrentStateProperty.BUCKET_SIZE.toString(), "" + bucketSize);
  // }
  // }

  @Override
  public boolean isValid() {
    if (getStateModelDefRef() == null) {
      LOG.error("Current state does not contain state model ref. id:" + getResourceName());
      return false;
    }
    if (getSessionId() == null) {
      LOG.error("CurrentState does not contain session id, id : " + getResourceName());
      return false;
    }
    return true;
  }

}
