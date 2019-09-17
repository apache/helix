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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * The factory class to create an instance of {@link ConstraintBasedAlgorithm}
 */
public class ConstraintBasedAlgorithmFactory {

  // TODO: the parameter comes from cluster config, will tune how these 2 integers will change the
  // soft constraint weight model
  public static RebalanceAlgorithm getInstance() {
    List<HardConstraint> hardConstraints =
        ImmutableList.of(new FaultZoneAwareConstraint(), new NodeCapacityConstraint(),
            new ReplicaActivateConstraint(), new NodeMaxPartitionLimitConstraint(),
            new ValidGroupTagConstraint(), new SamePartitionOnInstanceConstraint());

    Map<SoftConstraint, Float> softConstraints = ImmutableMap.<SoftConstraint, Float> builder()
        // TODO: merge with PartitionMovementConstraint
        // .put(new PartitionMovementConstraint(), 0.15f)
        .put(new InstancePartitionsCountConstraint(), 0.5f)
        .put(new ResourcePartitionAntiAffinityConstraint(), 0.1f)
        .put(new ResourceTopStateAntiAffinityConstraint(), 0.1f)
        .put(new MaxCapacityUsageInstanceConstraint(), 0.25f).build();

    return new ConstraintBasedAlgorithm(hardConstraints, softConstraints);
  }
}
