package org.apache.helix.controller.rebalancer.waged.constraints;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;
import java.util.Map;

import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.model.ClusterConfig;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * The factory class to create an instance of {@link ConstraintBasedAlgorithm}
 */
public class ConstraintBasedAlgorithmFactory {

  public static RebalanceAlgorithm getInstance(
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences, Float... weights) {
    List<HardConstraint> hardConstraints =
        ImmutableList.of(new FaultZoneAwareConstraint(), new NodeCapacityConstraint(),
            new ReplicaActivateConstraint(), new NodeMaxPartitionLimitConstraint(),
            new ValidGroupTagConstraint(), new SamePartitionOnInstanceConstraint());

    int evennessPreference =
        preferences.getOrDefault(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 1);
    int movementPreference =
        preferences.getOrDefault(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 1);
    float evennessRatio = (float) evennessPreference / (evennessPreference + movementPreference);
    float movementRatio = (float) movementPreference / (evennessPreference + movementPreference);

    Float[] defaults = {1f, 0.3f, 0.1f, 0.1f, 0.5f};
    Float[] w = weights.length == 0? defaults : weights;
    Map<SoftConstraint, Float> softConstraints = ImmutableMap.<SoftConstraint, Float> builder()
        .put(new PartitionMovementConstraint(), w[0] * movementRatio)
        .put(new InstancePartitionsCountConstraint(), w[1] * evennessRatio)
        .put(new ResourcePartitionAntiAffinityConstraint(), w[2] * evennessRatio)
        .put(new ResourceTopStateAntiAffinityConstraint(), w[3] * evennessRatio)
        .put(new MaxCapacityUsageInstanceConstraint(), w[4] * evennessRatio)
        .build();

    return new ConstraintBasedAlgorithm(hardConstraints, softConstraints);
  }
}
