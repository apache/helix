package org.apache.helix.api;

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
import java.util.Set;

import org.apache.helix.model.ResourceAssignment;

import com.google.common.collect.ImmutableMap;

public class RscAssignment {
  private final Map<PartitionId, Map<ParticipantId, State>> _resourceAssignment;

  /**
   * Construct an assignment from a physically-stored assignment
   * @param rscAssignment the assignment
   */
  public RscAssignment(ResourceAssignment rscAssignment) {
    Map<PartitionId, Map<ParticipantId, State>> resourceAssignment =
        new HashMap<PartitionId, Map<ParticipantId, State>>();

    for (org.apache.helix.model.Partition partition : rscAssignment.getMappedPartitions()) {
      Map<ParticipantId, State> replicaMap = new HashMap<ParticipantId, State>();
      Map<String, String> rawReplicaMap = rscAssignment.getReplicaMap(partition);
      for (String participantId : rawReplicaMap.keySet()) {
        replicaMap.put(new ParticipantId(participantId),
            new State(rawReplicaMap.get(participantId)));
      }
      resourceAssignment.put(new PartitionId(new ResourceId(rscAssignment.getResourceName()),
          partition.getPartitionName()), replicaMap);
    }

    _resourceAssignment = ImmutableMap.copyOf(resourceAssignment);
  }

  /**
   * Build an assignment from a map of assigned replicas
   * @param resourceAssignment map of (partition, participant, state)
   */
  public RscAssignment(Map<PartitionId, Map<ParticipantId, State>> resourceAssignment) {
    ImmutableMap.Builder<PartitionId, Map<ParticipantId, State>> mapBuilder =
        new ImmutableMap.Builder<PartitionId, Map<ParticipantId, State>>();
    for (PartitionId partitionId : resourceAssignment.keySet()) {
      mapBuilder.put(partitionId, ImmutableMap.copyOf(resourceAssignment.get(partitionId)));
    }
    _resourceAssignment = mapBuilder.build();
  }

  /**
   * Get the partitions currently with assignments
   * @return set of partition ids
   */
  public Set<PartitionId> getAssignedPartitions() {
    return _resourceAssignment.keySet();
  }

  /**
   * Get the replica assignment map for a partition
   * @param partitionId the partition to look up
   * @return map of (participant id, state)
   */
  public Map<ParticipantId, State> getParticipantStateMap(PartitionId partitionId) {
    return _resourceAssignment.get(partitionId);
  }

  /**
   * Assemble a full assignment
   */
  public static class Builder {
    private final Map<PartitionId, Map<ParticipantId, State>> _resourceAssignment;

    /**
     * Instantiate the builder
     */
    public Builder() {
      _resourceAssignment = new HashMap<PartitionId, Map<ParticipantId, State>>();
    }

    /**
     * Add assignments for a partition
     * @param partitionId partition to assign
     * @param replicaMap map of participant and state for each replica
     * @return Builder
     */
    public Builder addAssignments(PartitionId partitionId, Map<ParticipantId, State> replicaMap) {
      if (!_resourceAssignment.containsKey(partitionId)) {
        _resourceAssignment.put(partitionId, replicaMap);
      } else {
        _resourceAssignment.get(partitionId).putAll(replicaMap);
      }
      return this;
    }

    /**
     * Assign a single replica
     * @param partitionId partition to assign
     * @param participantId participant to host the replica
     * @param state replica state
     * @return Builder
     */
    public Builder addAssignment(PartitionId partitionId, ParticipantId participantId, State state) {
      Map<ParticipantId, State> replicaMap;
      if (!_resourceAssignment.containsKey(partitionId)) {
        replicaMap = new HashMap<ParticipantId, State>();
        _resourceAssignment.put(partitionId, replicaMap);
      } else {
        replicaMap = _resourceAssignment.get(partitionId);
      }
      replicaMap.put(participantId, state);
      return this;
    }

    /**
     * Build the resource assignment
     * @return instantiated RscAssignment
     */
    public RscAssignment build() {
      return new RscAssignment(_resourceAssignment);
    }
  }
}
