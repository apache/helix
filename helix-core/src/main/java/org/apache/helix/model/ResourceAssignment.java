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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Represents the assignments of replicas for an entire resource, keyed on partitions of the
 * resource. Each partition has its replicas assigned to a node, and each replica is in a state.
 * For example, if there is a partition p with 2 replicas, a valid assignment is:<br />
 * <br />
 * p: {(n1, s1), (n2, s2)}<br />
 * <br />
 * This means one replica of p is located at node n1 and is in state s1, and another is in node n2
 * and is in state s2. n1 cannot be equal to n2, but s1 can be equal to s2 if at least two replicas
 * can be in s1.
 */
public class ResourceAssignment extends HelixProperty {
  /**
   * Initialize an empty mapping
   * @param resourceId the resource being mapped
   */
  public ResourceAssignment(ResourceId resourceId) {
    super(resourceId.stringify());
  }

  /**
   * Instantiate from a record. This supports reading the assignment directly from the backing store
   * @param record backing record
   */
  public ResourceAssignment(ZNRecord record) {
    super(record);
  }

  /**
   * Get the resource for which this assignment was created
   * @return resource id
   */
  public ResourceId getResourceId() {
    return ResourceId.from(getId());
  }

  /**
   * Get the currently mapped partitions
   * @return list of Partition objects (immutable)
   */
  public List<? extends PartitionId> getMappedPartitionIds() {
    ImmutableList.Builder<PartitionId> builder = new ImmutableList.Builder<PartitionId>();
    for (String partitionName : _record.getMapFields().keySet()) {
      builder.add(PartitionId.from(partitionName));
    }
    return builder.build();
  }

  /**
   * Get the entire map of a resource
   * @return map of partition to participant to state
   */
  public Map<? extends PartitionId, Map<ParticipantId, State>> getResourceMap() {
    return replicaMapsFromStringMaps(_record.getMapFields());
  }

  /**
   * Get the participant, state pairs for a partition
   * @param partition the Partition to look up
   * @return map of (participant id, state)
   */
  public Map<ParticipantId, State> getReplicaMap(PartitionId partitionId) {
    Map<String, String> rawReplicaMap = _record.getMapField(partitionId.stringify());
    Map<ParticipantId, State> replicaMap = Maps.newHashMap();
    if (rawReplicaMap != null) {
      for (String participantName : rawReplicaMap.keySet()) {
        replicaMap.put(ParticipantId.from(participantName),
            State.from(rawReplicaMap.get(participantName)));
      }
    }
    return replicaMap;
  }

  /**
   * Add participant, state pairs for a partition
   * @param partitionId the partition to set
   * @param replicaMap map of (participant name, state)
   */
  public void addReplicaMap(PartitionId partitionId, Map<ParticipantId, State> replicaMap) {
    Map<String, String> convertedMap = Maps.newHashMap();
    for (ParticipantId participantId : replicaMap.keySet()) {
      convertedMap.put(participantId.stringify(), replicaMap.get(participantId).toString());
    }
    _record.setMapField(partitionId.stringify(), convertedMap);
  }

  /**
   * Helper for converting a map of strings to a concrete replica map
   * @param rawMap map of participant name to state name
   * @return map of participant id to state
   */
  public static Map<ParticipantId, State> replicaMapFromStringMap(Map<String, String> rawMap) {
    if (rawMap == null) {
      return Collections.emptyMap();
    }
    Map<ParticipantId, State> replicaMap = Maps.newHashMap();
    for (String participantName : rawMap.keySet()) {
      replicaMap.put(ParticipantId.from(participantName), State.from(rawMap.get(participantName)));
    }
    return replicaMap;
  }

  /**
   * Convert a full replica mapping as strings into participant state maps
   * @param rawMaps the map of partition name to participant name and state
   * @return converted maps
   */
  public static Map<PartitionId, Map<ParticipantId, State>> replicaMapsFromStringMaps(
      Map<String, Map<String, String>> rawMaps) {
    if (rawMaps == null) {
      return Collections.emptyMap();
    }
    Map<PartitionId, Map<ParticipantId, State>> participantStateMaps = Maps.newHashMap();
    for (String partitionId : rawMaps.keySet()) {
      participantStateMaps.put(PartitionId.from(partitionId),
          replicaMapFromStringMap(rawMaps.get(partitionId)));
    }
    return participantStateMaps;
  }

  /**
   * Helper for converting a replica map to a map of strings
   * @param replicaMap map of participant id to state
   * @return map of participant name to state name
   */
  public static Map<String, String> stringMapFromReplicaMap(Map<ParticipantId, State> replicaMap) {
    if (replicaMap == null) {
      return Collections.emptyMap();
    }
    Map<String, String> rawMap = new HashMap<String, String>();
    for (ParticipantId participantId : replicaMap.keySet()) {
      rawMap.put(participantId.stringify(), replicaMap.get(participantId).toString());
    }
    return rawMap;
  }

  /**
   * Convert a full state mapping into a mapping of string names
   * @param replicaMaps the map of partition id to participant id and state
   * @return converted maps
   */
  public static Map<String, Map<String, String>> stringMapsFromReplicaMaps(
      Map<? extends PartitionId, Map<ParticipantId, State>> replicaMaps) {
    if (replicaMaps == null) {
      return Collections.emptyMap();
    }
    Map<String, Map<String, String>> rawMaps = Maps.newHashMap();
    for (PartitionId partitionId : replicaMaps.keySet()) {
      rawMaps.put(partitionId.stringify(), stringMapFromReplicaMap(replicaMaps.get(partitionId)));
    }
    return rawMaps;
  }
}
