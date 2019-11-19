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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceSoftConstraint;
import org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider;
import org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.rebalancer.util.ResourceUsageCalculator;

public class PartitionWeightAwareEvennessConstraint extends AbstractRebalanceSoftConstraint {
  private final PartitionWeightProvider _partitionWeightProvider;
  private final CapacityProvider _capacityProvider;
  // Use to track any assignments that are proposed during the rebalance process.
  // Note these assignments are not reflected in providers.
  private final Map<String, Integer> _pendingUsage;

  public PartitionWeightAwareEvennessConstraint(PartitionWeightProvider partitionWeightProvider,
      CapacityProvider capacityProvider) {
    super();
    _partitionWeightProvider = partitionWeightProvider;
    _capacityProvider = capacityProvider;
    _pendingUsage = new HashMap<>();
  }

  /**
   * @return Remaining capacity in percentage.
   * Accuracy of evenness using this constraint is 1/100.
   */
  private int evaluate(String resource, String partition, String participant) {
    double capacity = _capacityProvider.getParticipantCapacity(participant);
    if (capacity == 0) {
      return 0;
    }
    double usage = _capacityProvider.getParticipantUsage(participant) + (_pendingUsage
        .containsKey(participant) ? _pendingUsage.get(participant) : 0);
    double available =
        capacity - usage - _partitionWeightProvider.getPartitionWeight(resource, partition);
    return (int) Math.ceil((available / capacity) * 100);
  }

  @Override
  public Map<String, int[]> evaluate(String resource, Map<String, String[]> proposedAssignment) {
    Map<String, int[]> result = new HashMap<>();
    for (String partition : proposedAssignment.keySet()) {
      String[] participants = proposedAssignment.get(partition);
      int[] evaluateResults = new int[participants.length];
      for (int i = 0; i < participants.length; i++) {
        evaluateResults[i] = evaluate(resource, partition, participants[i]);
      }
      result.put(partition, evaluateResults);
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
