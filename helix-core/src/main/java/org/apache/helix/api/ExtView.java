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

import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * External view of a resource
 */
public class ExtView {
  private final ResourceId _resourceId;

  /**
   * map of partition-id to map of participant-id to state
   */
  private final RscAssignment _extView;

  /**
   * Construct external view
   * @param stateMap map of partition-id to map of participant-id to state
   */
  public ExtView(ResourceId resourceId, Map<String, Map<String, String>> stateMap) {
    _resourceId = resourceId;

    // TODO convert to external view
    _extView = null;
  }

  /**
   * Get the state of a partition for a participant
   * @param partitionId
   * @param participantIds
   * @return the state or null if not exist
   */
  public State getState(PartitionId partitionId, ParticipantId participantId) {
    Map<ParticipantId, State> participantStateMap = _extView.getParticipantStateMap(partitionId);
    if (participantStateMap != null) {
      return participantStateMap.get(participantId);
    }
    return null;
  }

}
