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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.helix.HelixManagerProperties;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.model.ClusterConfig;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * The factory class to create an instance of {@link ConstraintBasedAlgorithm}
 */
public class ConstraintBasedAlgorithmFactory {
  // Evenness constraints tend to score within a smaller range.
  // In order to let their scores cause enough difference in the final evaluation result, we need to
  // enlarge the overall weight of the evenness constraints compared with the movement constraint.
  // TODO: Tune or make the following factor configurable.
  private static final int EVENNESS_PREFERENCE_NORMALIZE_FACTOR = 50;
  private static final Map<String, Float> MODEL = new HashMap() {
    {
      // The default setting
      put(PartitionMovementConstraint.class.getSimpleName(), 1f);
      put(InstancePartitionsCountConstraint.class.getSimpleName(), 0.3f);
      put(ResourcePartitionAntiAffinityConstraint.class.getSimpleName(), 0.1f);
      put(ResourceTopStateAntiAffinityConstraint.class.getSimpleName(), 0.1f);
      put(MaxCapacityUsageInstanceConstraint.class.getSimpleName(), 0.5f);
    }
  };

  static {
    Properties properties =
        new HelixManagerProperties(SystemPropertyKeys.SOFT_CONSTRAINT_WEIGHTS).getProperties();
    // overwrite the default value with data load from property file
    properties.forEach((constraintName, weight) -> MODEL.put(String.valueOf(constraintName),
        Float.valueOf(String.valueOf(weight))));
  }

  public static RebalanceAlgorithm getInstance(
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences) {
    List<HardConstraint> hardConstraints =
        ImmutableList.of(new FaultZoneAwareConstraint(), new NodeCapacityConstraint(),
            new ReplicaActivateConstraint(), new NodeMaxPartitionLimitConstraint(),
            new ValidGroupTagConstraint(), new SamePartitionOnInstanceConstraint());

    int evennessPreference =
        preferences.getOrDefault(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 1)
            * EVENNESS_PREFERENCE_NORMALIZE_FACTOR;
    int movementPreference =
        preferences.getOrDefault(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 1);
    float evennessRatio = (float) evennessPreference / (evennessPreference + movementPreference);
    float movementRatio = (float) movementPreference / (evennessPreference + movementPreference);

    List<SoftConstraint> softConstraints = ImmutableList.of(new PartitionMovementConstraint(),
        new InstancePartitionsCountConstraint(), new ResourcePartitionAntiAffinityConstraint(),
        new ResourceTopStateAntiAffinityConstraint(), new MaxCapacityUsageInstanceConstraint());
    Map<SoftConstraint, Float> softConstraintsWithWeight = Maps.toMap(softConstraints, key -> {
      String name = key.getClass().getSimpleName();
      float weight = MODEL.get(name);
      return name.equals(PartitionMovementConstraint.class.getSimpleName()) ? movementRatio * weight
          : evennessRatio * weight;
    });

    return new ConstraintBasedAlgorithm(hardConstraints, softConstraintsWithWeight);
  }
}
