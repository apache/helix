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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixProperty;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Customized states of partitions in a resource for an instance.
 */
public class CustomizedState extends HelixProperty {
  private static Logger LOG = LoggerFactory.getLogger(CustomizedState.class);

  /**
   * Lookup keys for the customized state
   */
  public enum CustomizedStateProperty {
    PREVIOUS_STATE,
    CURRENT_STATE,
    START_TIME,
    END_TIME
  }

  /**
   * Instantiate a customized state with a resource
   * @param resourceName name identifying the resource
   */
  public CustomizedState(String resourceName) {
    super(resourceName);
  }

  /**
   * Instantiate a customized state with a pre-populated ZNRecord
   * @param record a ZNRecord corresponding to the customized state
   */
  public CustomizedState(ZNRecord record) {
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
   * Get the partitions on this instance and the specified property that each partition is currently having.
   * @return (partition, property) pairs
   */
  public Map<String, String> getPartitionStateMap(CustomizedStateProperty property) {
    Map<String, String> map = new HashMap<String, String>();
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    for (String partitionName : mapFields.keySet()) {
      Map<String, String> tempMap = mapFields.get(partitionName);
      if (tempMap != null) {
        map.put(partitionName, tempMap.get(property.name()));
      }
    }
    return map;
  }

  /**
   * Get the state of a partition on this instance
   * @param partitionName the name of the partition
   * @return the state, or null if the partition is not present
   */
  public String getState(String partitionName) {
    return getProperty(partitionName, CustomizedStateProperty.CURRENT_STATE);
  }

  public long getStartTime(String partitionName) {
    String startTime = getProperty(partitionName, CustomizedStateProperty.START_TIME);
    return startTime == null ? -1L : Long.parseLong(startTime);
  }

  public long getEndTime(String partitionName) {
    String endTime = getProperty(partitionName, CustomizedStateProperty.END_TIME);
    return endTime == null ? -1L : Long.parseLong(endTime);
  }


  public String getPreviousState(String partitionName) {
    return getProperty(partitionName, CustomizedStateProperty.PREVIOUS_STATE);
  }

  private String getProperty(String partitionName, CustomizedStateProperty property) {
    Map<String, String> mapField = _record.getMapField(partitionName);
    if (mapField != null) {
      return mapField.get(property.name());
    }
    return null;
  }

  /**
   * Set the state that a partition is currently in on this instance
   * @param partitionName the name of the partition
   * @param state the state of the partition
   */
  public void setState(String partitionName, String state) {
    setProperty(partitionName, CustomizedStateProperty.CURRENT_STATE, state);
  }

  public void setStartTime(String partitionName, long startTime) {
    setProperty(partitionName, CustomizedStateProperty.START_TIME, String.valueOf(startTime));
  }

  public void setEndTime(String partitionName, long endTime) {
    setProperty(partitionName, CustomizedStateProperty.END_TIME, String.valueOf(endTime));
  }

  public void setPreviousState(String partitionName, String state) {
    setProperty(partitionName, CustomizedStateProperty.PREVIOUS_STATE, state);
  }

  private void setProperty(String partitionName, CustomizedStateProperty property, String value) {
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    mapFields.putIfAbsent(partitionName, new TreeMap<String, String>());
    mapFields.get(partitionName).put(property.name(), value);
  }

  @Override
  public boolean isValid() {
    if (getPartitionStateMap(CustomizedStateProperty.CURRENT_STATE) == null) {
      LOG.error("Customized state does not contain state map. id:" + getResourceName());
      return false;
    }
    return true;
  }
}
