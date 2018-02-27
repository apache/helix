package org.apache.helix.controller.rebalancer.constraint;

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

import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceHardConstraint;
import org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider;
import org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.rebalancer.util.ResourceUsageCalculator;

import java.util.HashMap;
import java.util.Map;

public class TotalCapacityConstraint extends AbstractRebalanceHardConstraint {
  private final PartitionWeightProvider _partitionWeightProvider;
  private final CapacityProvider _capacityProvider;
  // Use to track any assignments that are proposed during the rebalance process.
  // Note these assignments are not reflected in providers.
  private final Map<String, Integer> _pendingUsage;

  public TotalCapacityConstraint(PartitionWeightProvider partitionWeightProvider,
      CapacityProvider capacityProvider) {
    super();
    _partitionWeightProvider = partitionWeightProvider;
    _capacityProvider = capacityProvider;
    _pendingUsage = new HashMap<>();
  }

  private boolean validate(String resource, String partition, String participant) {
    int usage = _capacityProvider.getParticipantUsage(participant) + (_pendingUsage
        .containsKey(participant) ? _pendingUsage.get(participant) : 0);
    return _partitionWeightProvider.getPartitionWeight(resource, partition) + usage
        <= _capacityProvider.getParticipantCapacity(participant);
  }

  @Override
  public Map<String, boolean[]> isValid(String resource, Map<String, String[]> proposedAssignment) {
    Map<String, boolean[]> result = new HashMap<>();
    for (String partition : proposedAssignment.keySet()) {
      String[] participants = proposedAssignment.get(partition);
      boolean[] validateResults = new boolean[participants.length];
      for (int i = 0; i < participants.length; i++) {
        validateResults[i] = validate(resource, partition, participants[i]);
      }
      result.put(partition, validateResults);
    }
    return result;
  }

  @Override
  public void updateAssignment(ResourcesStateMap pendingAssignment) {
    Map<String, Integer> newParticipantUsage =
        ResourceUsageCalculator.getResourceUsage(pendingAssignment, _partitionWeightProvider);
    for (String participant : newParticipantUsage.keySet()) {
      if (!_pendingUsage.containsKey(participant)) {
        _pendingUsage.put(participant, 0);
      }
      _pendingUsage
          .put(participant, _pendingUsage.get(participant) + newParticipantUsage.get(participant));
    }
  }
}
