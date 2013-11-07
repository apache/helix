package org.apache.helix.model.builder;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.ResourceAssignment;

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

/**
 * Build an ideal assignment of resources
 */
public class ResourceAssignmentBuilder {
  private final ResourceId _resourceId;
  private final Map<PartitionId, Map<ParticipantId, State>> _mapping;

  /**
   * Create an assignment for a given resource
   * @param resourceId resource id
   */
  public ResourceAssignmentBuilder(ResourceId resourceId) {
    _resourceId = resourceId;
    _mapping = new HashMap<PartitionId, Map<ParticipantId, State>>();
  }

  /**
   * Add multiple assignments of partition replicas
   * @param partitionId the partition to assign
   * @param replicaMap participant-state map of assignments
   * @return ResourceAssignmentBuilder
   */
  public ResourceAssignmentBuilder addAssignments(PartitionId partitionId,
      Map<ParticipantId, State> replicaMap) {
    if (!_mapping.containsKey(partitionId)) {
      _mapping.put(partitionId, new HashMap<ParticipantId, State>());
    }
    _mapping.get(partitionId).putAll(replicaMap);
    return this;
  }

  /**
   * Add a single replica assignment
   * @param partitonId the partition to assign
   * @param participantId participant of assignment
   * @param state replica state
   * @return ResourceAssignmentBuilder
   */
  public ResourceAssignmentBuilder addAssignment(PartitionId partitonId,
      ParticipantId participantId, State state) {
    Map<ParticipantId, State> replicaMap;
    if (!_mapping.containsKey(partitonId)) {
      replicaMap = new HashMap<ParticipantId, State>();
      _mapping.put(partitonId, replicaMap);
    } else {
      replicaMap = _mapping.get(partitonId);
    }
    replicaMap.put(participantId, state);
    return this;
  }

  /**
   * Get a complete resource assignment
   * @return ResourceAssignment
   */
  public ResourceAssignment build() {
    ResourceAssignment assignment = new ResourceAssignment(_resourceId);
    for (PartitionId partitionId : _mapping.keySet()) {
      assignment.addReplicaMap(partitionId, _mapping.get(partitionId));
    }
    return assignment;
  }
}
