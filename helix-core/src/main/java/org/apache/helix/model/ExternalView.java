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
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

/**
 * External view is an aggregation (across all instances)
 * of current states for the partitions in a resource
 */
public class ExternalView extends HelixProperty {
  /**
   * Instantiate an external view with the resource it corresponds to
   * @param resource the name of the resource
   */
  public ExternalView(String resource) {
    super(new ZNRecord(resource));
  }

  /**
   * Instantiate an external view with a pre-populated record
   * @param record ZNRecord corresponding to an external view
   */
  public ExternalView(ZNRecord record) {
    super(record);
  }

  /**
   * For a given replica, specify which partition it corresponds to, where it is served, and its
   * current state
   * @param partition the partition of the replica being served
   * @param instance the instance serving the replica
   * @param state the state the replica is in
   */
  public void setState(String partition, String instance, String state) {
    if (_record.getMapField(partition) == null) {
      _record.setMapField(partition, new TreeMap<String, String>());
    }
    _record.getMapField(partition).put(instance, state);
  }

  /**
   * For a given partition, indicate where and in what state each of its replicas is in
   * @param partitionName the partition to set
   * @param currentStateMap (instance, state) pairs
   */
  public void setStateMap(String partitionName, Map<String, String> currentStateMap) {
    _record.setMapField(partitionName, currentStateMap);
  }

  /**
   * Get all the partitions of the resource
   * @return a set of partition names
   */
  public Set<String> getPartitionSet() {
    return _record.getMapFields().keySet();
  }

  /**
   * Get the instance and the state for each partition replica
   * @param partitionName the partition to look up
   * @return (instance, state) pairs
   */
  public Map<String, String> getStateMap(String partitionName) {
    return _record.getMapField(partitionName);
  }

  /**
   * Get the resource represented by this view
   * @return the name of the resource
   */
  public String getResourceName() {
    return _record.getId();
  }

  @Override
  public boolean isValid() {
    return true;
  }
}
