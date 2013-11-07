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
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
   * Instantiate an external view with the resource it corresponds to
   * @param resource the id of the resource
   */
  public ExternalView(ResourceId resource) {
    super(new ZNRecord(resource.stringify()));
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
   * For a given replica, specify which partition it corresponds to, where it is served, and its
   * current state
   * @param partitionId the partition of the replica being served
   * @param participantId the instance serving the replica
   * @param state the state the replica is in
   */
  public void setState(PartitionId partitionId, ParticipantId participantId, State state) {
    if (_record.getMapField(partitionId.stringify()) == null) {
      _record.setMapField(partitionId.stringify(), new TreeMap<String, String>());
    }
    _record.getMapField(partitionId.stringify()).put(participantId.stringify(), state.toString());
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
   * For a given partition, indicate where and in what state each of its replicas is in
   * @param partitionId the partition to set
   * @param currentStateMap (participant, state) pairs
   */
  public void setStateMap(PartitionId partitionId, Map<ParticipantId, State> currentStateMap) {
    for (ParticipantId participantId : currentStateMap.keySet()) {
      setState(partitionId, participantId, currentStateMap.get(participantId));
    }
  }

  /**
   * Get all the partitions of the resource
   * @return a set of partition names
   */
  public Set<String> getPartitionSet() {
    return _record.getMapFields().keySet();
  }

  /**
   * Get all the partitions of the resource
   * @return a set of partition ids
   */
  public Set<PartitionId> getPartitionIdSet() {
    Set<PartitionId> partitionSet = Sets.newHashSet();
    for (String partitionName : getPartitionSet()) {
      partitionSet.add(PartitionId.from(partitionName));
    }
    return partitionSet;
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
   * Get the participant and the state for each partition replica
   * @param partitionId the partition to look up
   * @return (participant, state) pairs
   */
  public Map<ParticipantId, State> getStateMap(PartitionId partitionId) {
    Map<String, String> rawStateMap = getStateMap(partitionId.stringify());
    if (rawStateMap == null) {
      return null;
    }
    Map<ParticipantId, State> stateMap = Maps.newHashMap();
    for (String participantName : rawStateMap.keySet()) {
      stateMap.put(ParticipantId.from(participantName),
          State.from(rawStateMap.get(participantName)));
    }
    return stateMap;
  }

  /**
   * Get the resource represented by this view
   * @return the name of the resource
   */
  public String getResourceName() {
    return _record.getId();
  }

  /**
   * Get the resource represented by this view
   * @return resource id
   */
  public ResourceId getResourceId() {
    return ResourceId.from(getResourceName());
  }

  @Override
  public boolean isValid() {
    return true;
  }

  /**
   * Convert a partition mapping as strings into a participant state map
   * @param rawMap the map of participant name to state
   * @return converted map
   */
  public static Map<ParticipantId, State> stateMapFromStringMap(Map<String, String> rawMap) {
    return ResourceAssignment.replicaMapFromStringMap(rawMap);
  }
}
