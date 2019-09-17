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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * The class retrieves the offline model that defines the relative importance of soft constraints.
 */
class SoftConstraintWeightModel {
  private final Map<SoftConstraint, Float> _model;

  SoftConstraintWeightModel() {
    // TODO: add additional soft constraints and adjust the weight
    _model = ImmutableMap.<SoftConstraint, Float> builder()
        .put(new InstancePartitionsCountConstraint(), 1.0f)
        .put(new ResourcePartitionAntiAffinityConstraint(), 1.0f)
        .put(new ResourceTopStateAntiAffinityConstraint(), 1.0f)
        .put(new MaxCapacityUsageInstanceConstraint(), 1.0f).build();
  }

  List<SoftConstraint> getSoftConstraints() {
    return new ArrayList<>(_model.keySet());
  }

  /**
   * Get the sum of normalized scores, given calculated scores map of soft constraints
   * @param originScoresMap The origin scores by soft constraints
   * @return The sum of soft constraints scores
   */
  float getSumOfScores(Map<SoftConstraint, Float> originScoresMap) {
    float sum = 0;
    for (Map.Entry<SoftConstraint, Float> softConstraintScoreEntry : originScoresMap.entrySet()) {
      SoftConstraint softConstraint = softConstraintScoreEntry.getKey();
      float weight = _model.get(softConstraint);
      sum += softConstraintScoreEntry.getValue() * weight;
    }

    return sum;
  }
}
