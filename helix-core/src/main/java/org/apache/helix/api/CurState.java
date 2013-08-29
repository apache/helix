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

import org.apache.helix.model.CurrentState;

import com.google.common.collect.ImmutableMap;

/**
 * Current state per participant per resource
 */
public class CurState {
  private final ResourceId _resourceId;
  private final ParticipantId _participantId;

  /**
   * map of partition-id to state
   */
  final Map<PartitionId, State> _stateMap;

  /**
   * Construct current state
   * @param resource
   * @param participant
   * @param currentState
   */
  public CurState(ResourceId resourceId, ParticipantId participantId, CurrentState currentState) {
    _resourceId = resourceId;
    _participantId = participantId;

    Map<PartitionId, State> stateMap = new HashMap<PartitionId, State>();
    Map<String, String> currentStateMap = currentState.getPartitionStateStringMap();
    for (String partitionId : currentStateMap.keySet()) {
      String state = currentStateMap.get(partitionId);
      stateMap.put(new PartitionId(resourceId, PartitionId.stripResourceId(partitionId)),
          new State(state));
    }
    _stateMap = ImmutableMap.copyOf(stateMap);
  }

  /**
   * Get current state for a partition
   * @param partition-id
   * @return state of the partition or null if partition not exist
   */
  public State getState(PartitionId partitionId) {
    return _stateMap.get(partitionId);
  }

  /**
   * Get the set of partition-id's in the current state
   * @return set of partition-id's or empty set if none
   */
  public Set<PartitionId> getPartitionIdSet() {
    return _stateMap.keySet();
  }
}
