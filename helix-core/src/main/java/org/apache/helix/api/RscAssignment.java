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

import org.apache.helix.model.ResourceAssignment;

import com.google.common.collect.ImmutableMap;

public class RscAssignment {
  private final Map<PartitionId, Map<ParticipantId, State>> _resourceAssignment;

  public RscAssignment(ResourceAssignment rscAssignment) {
    Map<PartitionId, Map<ParticipantId, State>> resourceAssignment =
        new HashMap<PartitionId, Map<ParticipantId, State>>();

    // TODO fill the map

    _resourceAssignment = ImmutableMap.copyOf(resourceAssignment);
  }

  public Map<ParticipantId, State> getParticipantStateMap(PartitionId partitionId) {
    return _resourceAssignment.get(partitionId);
  }
}
