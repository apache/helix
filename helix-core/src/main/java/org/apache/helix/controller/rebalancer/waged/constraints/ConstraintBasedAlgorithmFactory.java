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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.model.ClusterConfig;

/**
 * The factory class to create an instance of {@link ConstraintBasedAlgorithm}
 */
public class ConstraintBasedAlgorithmFactory {
  private static final Map<String, Float> MODEL = new HashMap<String, Float>() {
    {
      // The default setting
      put(PartitionMovementConstraint.class.getSimpleName(), 2f);
      put(InstancePartitionsCountConstraint.class.getSimpleName(), 1f);
      put(ResourcePartitionAntiAffinityConstraint.class.getSimpleName(), 1f);
      put(ResourceTopStateAntiAffinityConstraint.class.getSimpleName(), 3f);
      put(MaxCapacityUsageInstanceConstraint.class.getSimpleName(), 5f);
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
        preferences.getOrDefault(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 1);
    int movementPreference =
        preferences.getOrDefault(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 1);

    List<SoftConstraint> softConstraints = ImmutableList
        .of(new PartitionMovementConstraint(), new InstancePartitionsCountConstraint(),
            new ResourcePartitionAntiAffinityConstraint(),
            new ResourceTopStateAntiAffinityConstraint(), new MaxCapacityUsageInstanceConstraint());
    Map<SoftConstraint, Float> softConstraintsWithWeight = Maps.toMap(softConstraints, key -> {
      String name = key.getClass().getSimpleName();
      float weight = MODEL.get(name);
      return name.equals(PartitionMovementConstraint.class.getSimpleName()) ?
          movementPreference * weight : evennessPreference * weight;
    });

    return new ConstraintBasedAlgorithm(hardConstraints, softConstraintsWithWeight);
  }
}
